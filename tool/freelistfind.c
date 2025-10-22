/*
** A utility for finding pages in the SQLite freelist that are actually in use.
**
** This tool:
** 1. Walks the freelist chain to collect all freelist pages
** 2. Walks all btrees to find all pages in use
** 3. Reports any pages that appear in both lists (the corruption)
**
** Usage: freelistfind DATABASE_FILE
*/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#if !defined(_MSC_VER)
#include <unistd.h>
#else
#include <io.h>
#endif

#include "sqlite3.h"

typedef unsigned char u8;
typedef unsigned int u32;
typedef sqlite3_int64 i64;

/*
** Constants derived from SQLite file format specification
** Reference: https://www.sqlite.org/fileformat.html
*/

/* Database header offsets and sizes (Section 1.3) */
#define SQLITE_HEADER_SIZE           100   /* Size of database file header */
#define SQLITE_HEADER_MAGIC_OFFSET   0     /* Offset to magic string */
#define SQLITE_HEADER_MAGIC_SIZE     16    /* Size of magic string */
#define SQLITE_HEADER_PAGESIZE_OFFSET 16   /* Offset to page size (2 bytes) */
#define SQLITE_HEADER_RESERVED_OFFSET 20   /* Offset to reserved space byte */
#define SQLITE_HEADER_FREELIST_OFFSET 32   /* Offset to first freelist trunk page */
#define SQLITE_HEADER_FREELIST_COUNT  36   /* Offset to total freelist page count */
#define SQLITE_HEADER_DB_FILESIZE     28   /* Offset to database size in pages */

/* Special page size values (Section 1.3.2) */
#define SQLITE_PAGESIZE_MAGIC_65536  1     /* Magic value representing 65536 bytes */
#define SQLITE_PAGESIZE_DEFAULT      1024  /* Default page size for value 0 */
#define SQLITE_PAGESIZE_MAX          65536 /* Maximum page size */

/* Page 1 special offset (Section 1.6) */
#define PAGE1_HEADER_OFFSET          100   /* Btree header starts at offset 100 on page 1 */

/* B-tree page types (Section 1.6) */
#define BTREE_INTERIOR_INDEX         2     /* Interior index b-tree page (0x02) */
#define BTREE_INTERIOR_TABLE         5     /* Interior table b-tree page (0x05) */
#define BTREE_LEAF_INDEX             10    /* Leaf index b-tree page (0x0a) */
#define BTREE_LEAF_TABLE             13    /* Leaf table b-tree page (0x0d) */

/* B-tree page header offsets (Section 1.6) */
#define BTREE_HEADER_PAGETYPE        0     /* Page type (1 byte) */
#define BTREE_HEADER_FREEBLOCK       1     /* First freeblock offset (2 bytes) */
#define BTREE_HEADER_NCELLS          3     /* Number of cells (2 bytes) */
#define BTREE_HEADER_CELL_OFFSET     5     /* Cell content area offset (2 bytes) */
#define BTREE_HEADER_NFRAGMENTS      7     /* Fragmented free bytes (1 byte) */
#define BTREE_HEADER_RIGHTCHILD      8     /* Rightmost child pointer (4 bytes, interior only) */
#define BTREE_HEADER_SIZE_INTERIOR   12    /* Header size for interior pages */
#define BTREE_HEADER_SIZE_LEAF       8     /* Header size for leaf pages */

/* Freelist structure offsets (Section 1.5) */
#define FREELIST_TRUNK_NEXT_OFFSET   0     /* Next trunk page pointer (4 bytes) */
#define FREELIST_TRUNK_COUNT_OFFSET  4     /* Leaf page count (4 bytes) */
#define FREELIST_TRUNK_LEAVES_OFFSET 8     /* Start of leaf page array */
#define FREELIST_TRUNK_HEADER_SIZE   8     /* Size of trunk page header */
#define FREELIST_LEAF_ENTRY_SIZE     4     /* Size of each leaf page entry */

/* Overflow page constants (Section 1.6) */
#define OVERFLOW_NEXT_OFFSET         0     /* Next overflow page pointer (4 bytes) */
#define OVERFLOW_HEADER_SIZE         4     /* Size of overflow page header */

/* Cell pointer size */
#define CELL_POINTER_SIZE            2     /* Each cell pointer is 2 bytes */
#define CHILD_POINTER_SIZE           4     /* Each child pointer is 4 bytes */

/* Payload calculation constants (Section 1.6) - for table b-tree leaf cells */
#define PAYLOAD_MIN_FRACTION         32    /* Numerator for minLocal calculation */
#define PAYLOAD_DIVISOR              255   /* Divisor for payload calculations */
#define PAYLOAD_MIN_SUBTRACT         23    /* Constant subtracted in minLocal */
#define PAYLOAD_MAX_SUBTRACT         35    /* U - 35 for table leaf maxLocal */
#define PAYLOAD_USABLE_SUBTRACT      12    /* U - 12 in payload formulas */

/* Safety limits */
#define MAX_BTREE_DEPTH              50    /* Maximum recursion depth */
#define MAX_FREELIST_CYCLE_CHECK     10000 /* Maximum pages to track for cycles */

/* Global state */
static struct {
  int fd;           /* File descriptor */
  u32 pagesize;     /* Page size from header */
  u32 mxPage;       /* Maximum page number */
  u32 firstFreelist; /* First freelist trunk page */
  u32 freelistCount; /* Freelist count from header */
  u32 reservedSpace; /* Reserved space at end of each page */
  u8 *inFreelist;   /* Bitmap: is page in freelist? */
  u8 *inUse;        /* Bitmap: is page in use by btree? */
} g;

/* Extract a big-endian 32-bit integer */
static u32 get32(const u8 *z){
  return (z[0]<<24) + (z[1]<<16) + (z[2]<<8) + z[3];
}

/* Read bytes from file at given offset */
static u8* readBytes(off_t offset, size_t size){
  u8 *buf = malloc(size);
  ssize_t n;

  if( !buf ){
    fprintf(stderr, "ERROR: out of memory\n");
    return NULL;
  }

  if( lseek(g.fd, offset, SEEK_SET) != offset ){
    fprintf(stderr, "ERROR: seek failed\n");
    free(buf);
    return NULL;
  }

  n = read(g.fd, buf, size);
  if( n != (ssize_t)size ){
    fprintf(stderr, "ERROR: read failed\n");
    free(buf);
    return NULL;
  }

  return buf;
}

/* Read a page from the database file */
static u8* readPage(u32 pgno){
  if( pgno<1 || pgno>g.mxPage ){
    fprintf(stderr, "ERROR: page %u out of range 1..%u\n", pgno, g.mxPage);
    return NULL;
  }

  return readBytes((off_t)(pgno-1) * g.pagesize, g.pagesize);
}

/* Read and parse the database header */
static int readHeader(void){
  u8 *header;
  off_t fileSize;

  header = readBytes(0, SQLITE_HEADER_SIZE);
  if( !header ) return 1;

  /* Check magic string "SQLite format 3\0" (Section 1.3.1) */
  if( memcmp(header + SQLITE_HEADER_MAGIC_OFFSET, 
             "SQLite format 3\000", SQLITE_HEADER_MAGIC_SIZE) != 0 ){
    fprintf(stderr, "ERROR: not a SQLite database file\n");
    free(header);
    return 1;
  }

  /* Extract page size (Section 1.3.2) - big-endian 16-bit at offset 16 */
  g.pagesize = (header[SQLITE_HEADER_PAGESIZE_OFFSET]<<8) | 
                header[SQLITE_HEADER_PAGESIZE_OFFSET+1];
  if( g.pagesize == SQLITE_PAGESIZE_MAGIC_65536 ){
    g.pagesize = SQLITE_PAGESIZE_MAX;  /* Special encoding for 65536 */
  }
  if( g.pagesize == 0 ){
    g.pagesize = SQLITE_PAGESIZE_DEFAULT;  /* Default for legacy files */
  }

  /* Get file size to calculate max page */
  fileSize = lseek(g.fd, 0, SEEK_END);
  if( fileSize < 0 ){
    fprintf(stderr, "ERROR: cannot determine file size\n");
    free(header);
    return 1;
  }
  g.mxPage = (u32)((fileSize + g.pagesize - 1) / g.pagesize);

  /* Extract freelist information (Section 1.3.8) */
  g.firstFreelist = get32(header + SQLITE_HEADER_FREELIST_OFFSET);
  g.freelistCount = get32(header + SQLITE_HEADER_FREELIST_COUNT);

  /* Extract reserved space per page (Section 1.3.4) */
  g.reservedSpace = header[SQLITE_HEADER_RESERVED_OFFSET];

  free(header);
  return 0;
}

/* Mark a page as being in the freelist */
static void markFreelist(u32 pgno){
  if( pgno >= 1 && pgno <= g.mxPage ){
    g.inFreelist[pgno] = 1;
  }
}

/* Mark a page as being in use */
static void markInUse(u32 pgno){
  if( pgno >= 1 && pgno <= g.mxPage ){
    g.inUse[pgno] = 1;
  }
}

/* Walk the freelist chain and mark all pages */
static int walkFreelist(void){
  u32 pgno = g.firstFreelist;
  u32 trunkCount = 0;
  u32 leafCount = 0;
  u32 visited[MAX_FREELIST_CYCLE_CHECK];
  u32 visitCount = 0;

  printf("Walking freelist...\n");

  while( pgno != 0 ){
    u8 *page;
    u32 nextTrunk;
    u32 numLeaves;
    u32 i;

    /* Check for cycles */
    for(i=0; i<visitCount; i++){
      if( visited[i] == pgno ){
        fprintf(stderr, "ERROR: cycle in freelist at page %u\n", pgno);
        return 1;
      }
    }
    if( visitCount < MAX_FREELIST_CYCLE_CHECK ){
      visited[visitCount++] = pgno;
    }

    /* Read trunk page */
    page = readPage(pgno);
    if( !page ) return 1;

    trunkCount++;
    markFreelist(pgno);

    /* Read trunk page structure (Section 1.5) */
    nextTrunk = get32(page + FREELIST_TRUNK_NEXT_OFFSET);
    numLeaves = get32(page + FREELIST_TRUNK_COUNT_OFFSET);

    /* Sanity check - max entries that fit after 8-byte header */
    u32 maxLeaves = (g.pagesize - FREELIST_TRUNK_HEADER_SIZE) / FREELIST_LEAF_ENTRY_SIZE;
    if( numLeaves > maxLeaves ){
      fprintf(stderr, "ERROR: trunk page %u has invalid leaf count\n", pgno);
      numLeaves = maxLeaves;
    }

    /* Mark leaf pages from array at offset 8 */
    for(i=0; i<numLeaves; i++){
      u32 leafPgno = get32(page + FREELIST_TRUNK_LEAVES_OFFSET + i*FREELIST_LEAF_ENTRY_SIZE);
      markFreelist(leafPgno);
      leafCount++;
    }

    free(page);
    pgno = nextTrunk;
  }

  printf("  Found %u trunk pages and %u leaf pages (total %u)\n",
         trunkCount, leafCount, trunkCount + leafCount);
  return 0;
}

/* Extract varint */
static int decodeVarint(const u8 *z, i64 *pVal){
  i64 v = 0;
  int i;
  for(i=0; i<8; i++){
    v = (v<<7) + (z[i]&0x7f);
    if( (z[i]&0x80)==0 ){ *pVal = v; return i+1; }
  }
  v = (v<<8) + (z[i]&0xff);
  *pVal = v;
  return 9;
}

/* Walk a btree page and mark pages as in use */
static void walkBtree(u32 pgno, int depth){
  u8 *page;
  int hdr;
  u8 pageType;
  u32 nCell;
  u32 i;
  int cellStart;

  if( pgno < 1 || pgno > g.mxPage ) return;
  if( g.inUse[pgno] ) return; /* Already visited */
  if( depth > MAX_BTREE_DEPTH ) return; /* Prevent deep recursion */

  markInUse(pgno);

  page = readPage(pgno);
  if( !page ) return;

  /* Page 1 has 100-byte database header before btree header (Section 1.6) */
  hdr = (pgno == 1) ? PAGE1_HEADER_OFFSET : 0;
  pageType = page[hdr + BTREE_HEADER_PAGETYPE];

  /* Only handle b-tree pages (Section 1.6) */
  if( pageType != BTREE_INTERIOR_INDEX && pageType != BTREE_INTERIOR_TABLE && 
      pageType != BTREE_LEAF_INDEX && pageType != BTREE_LEAF_TABLE ){
    free(page);
    return;
  }

  /* Read number of cells (2-byte big-endian at offset 3) */
  nCell = (page[hdr + BTREE_HEADER_NCELLS]<<8) | page[hdr + BTREE_HEADER_NCELLS + 1];

  /* Interior pages - walk child pointers (Section 1.6) */
  if( pageType == BTREE_INTERIOR_INDEX || pageType == BTREE_INTERIOR_TABLE ){
    cellStart = hdr + BTREE_HEADER_SIZE_INTERIOR;

    /* Walk each cell's left child pointer */
    for(i=0; i<nCell && i<g.pagesize/CELL_POINTER_SIZE; i++){
      u32 cellOffset = (page[cellStart + i*CELL_POINTER_SIZE]<<8) | 
                        page[cellStart + i*CELL_POINTER_SIZE + 1];
      if( cellOffset >= CHILD_POINTER_SIZE && cellOffset < g.pagesize ){
        u32 childPage = get32(page + cellOffset);
        walkBtree(childPage, depth + 1);
      }
    }

    /* Rightmost child pointer at offset 8 in header (4 bytes before cellStart) */
    if( cellStart >= CHILD_POINTER_SIZE ){
      u32 rightChild = get32(page + hdr + BTREE_HEADER_RIGHTCHILD);
      walkBtree(rightChild, depth + 1);
    }
  }

  /* Check for overflow pages in leaf cells (Section 1.6) */
  if( pageType == BTREE_LEAF_TABLE || pageType == BTREE_LEAF_INDEX ){
    cellStart = hdr + BTREE_HEADER_SIZE_LEAF;

    for(i=0; i<nCell && i<g.pagesize/CELL_POINTER_SIZE; i++){
      u32 cellOffset = (page[cellStart + i*CELL_POINTER_SIZE]<<8) | 
                        page[cellStart + i*CELL_POINTER_SIZE + 1];
      if( cellOffset < g.pagesize - CHILD_POINTER_SIZE ){
        u8 *cell = page + cellOffset;
        i64 nPayload;
        i64 rowid;
        int n;

        /* Table leaf cell format (Section 1.6):
         *   1. Payload size (varint)
         *   2. Rowid (varint)
         *   3. Payload data
         * Index leaf cell format:
         *   1. Payload size (varint)
         *   2. Payload data (no rowid)
         */
        
        /* Get payload size - first varint in both table and index leaf cells */
        n = decodeVarint(cell, &nPayload);
        cell += n;

        /* Skip rowid varint for table leaf (not present in index leaf) */
        if( pageType == BTREE_LEAF_TABLE ){
          n = decodeVarint(cell, &rowid);
          cell += n;
        }
        
        /* Validate payload size is reasonable before checking for overflow */
        /* SQLite max BLOB/TEXT is ~2GB, but realistic cells are much smaller */
        /* Use 1GB as upper bound to catch bogus varint decoding */
        if( nPayload < 0 || nPayload > 1073741824 /* 1GB */ ){
          /* Payload size is unreasonable - probably corrupt or misread varint */
          continue;
        }

        /* Check if there are overflow pages (Section 1.6) */
        /* Use reserved space from database header (Section 1.3.4) */
        u32 usable = g.pagesize - g.reservedSpace;
        u32 maxLocal = usable - PAYLOAD_MAX_SUBTRACT;
        if( pageType == BTREE_LEAF_TABLE ){
          u32 minLocal = ((usable - PAYLOAD_USABLE_SUBTRACT) * PAYLOAD_MIN_FRACTION 
                          / PAYLOAD_DIVISOR) - PAYLOAD_MIN_SUBTRACT;
          if( nPayload > maxLocal ){
            /* Has overflow - find first overflow page pointer after local payload */
            u32 localSize = minLocal + ((nPayload - minLocal) % (usable - OVERFLOW_HEADER_SIZE));
            if( (u32)(cell - page + localSize + CHILD_POINTER_SIZE) <= g.pagesize ){
              u32 overflowPage = get32(cell + localSize);
              /* Validate overflow page number before following it */
              if( overflowPage > 0 && overflowPage <= g.mxPage ){
                /* Walk overflow chain */
                while( overflowPage > 0 && overflowPage <= g.mxPage ){
                  markInUse(overflowPage);
                  u8 *ovfl = readPage(overflowPage);
                  if( !ovfl ) break;
                  overflowPage = get32(ovfl + OVERFLOW_NEXT_OFFSET);
                  free(ovfl);
                }
              }
            }
          }
        }
      }
    }
  }

  free(page);
}

/* Walk all btrees via sqlite_schema */
static int walkAllBtrees(const char *dbPath){
  sqlite3 *db;
  sqlite3_stmt *stmt;
  int rc;

  printf("Walking all btrees...\n");

  /* Mark page 1 (schema page) */
  markInUse(1);
  walkBtree(1, 0);

  /* Open database */
  rc = sqlite3_open_v2(dbPath, &db, SQLITE_OPEN_READONLY, NULL);
  if( rc != SQLITE_OK ){
    fprintf(stderr, "ERROR: cannot open database: %s\n", sqlite3_errmsg(db));
    sqlite3_close(db);
    return 1;
  }

  /* Query all root pages */
  rc = sqlite3_prepare_v2(db,
    "SELECT name, rootpage FROM sqlite_master WHERE rootpage > 0",
    -1, &stmt, NULL);

  if( rc != SQLITE_OK ){
    fprintf(stderr, "ERROR: cannot query schema: %s\n", sqlite3_errmsg(db));
    sqlite3_close(db);
    return 1;
  }

  while( sqlite3_step(stmt) == SQLITE_ROW ){
    const char *name = (const char*)sqlite3_column_text(stmt, 0);
    u32 rootpage = (u32)sqlite3_column_int(stmt, 1);
    printf("  Walking %s (root page %u)\n", name, rootpage);
    walkBtree(rootpage, 0);
  }

  sqlite3_finalize(stmt);
  sqlite3_close(db);

  return 0;
}

/* Find and report conflicts */
static void findConflicts(void){
  u32 i;
  u32 conflicts = 0;

  printf("\n=== CHECKING FOR CONFLICTS ===\n");

  for(i=1; i<=g.mxPage; i++){
    if( g.inFreelist[i] && g.inUse[i] ){
      printf("CONFLICT: Page %u is in BOTH freelist AND in use!\n", i);
      conflicts++;
    }
  }

  if( conflicts == 0 ){
    printf("No conflicts found - freelist and in-use pages are disjoint.\n");
  }else{
    printf("\nTotal conflicts: %u\n", conflicts);
    printf("\nThis means %u page(s) in the freelist are actually in use.\n", conflicts);
    printf("This is the corruption causing the integrity_check error.\n");
  }
}

/* Main */
int main(int argc, char **argv){
  if( argc != 2 ){
    fprintf(stderr, "Usage: %s DATABASE_FILE\n", argv[0]);
    return 1;
  }

  /* Open database file */
  g.fd = open(argv[1], O_RDONLY);
  if( g.fd < 0 ){
    fprintf(stderr, "ERROR: cannot open '%s'\n", argv[1]);
    return 1;
  }

  /* Read header */
  if( readHeader() ){
    close(g.fd);
    return 1;
  }

  printf("Database: %s\n", argv[1]);
  printf("Page size: %u bytes\n", g.pagesize);
  printf("Total pages: %u\n", g.mxPage);
  printf("\n");

  /* Allocate bitmaps */
  g.inFreelist = calloc(g.mxPage + 1, 1);
  g.inUse = calloc(g.mxPage + 1, 1);
  if( !g.inFreelist || !g.inUse ){
    fprintf(stderr, "ERROR: out of memory\n");
    close(g.fd);
    return 1;
  }

  /* Walk freelist */
  if( walkFreelist() ){
    close(g.fd);
    return 1;
  }

  printf("\n");

  /* Walk all btrees */
  if( walkAllBtrees(argv[1]) ){
    close(g.fd);
    return 1;
  }

  printf("\n");

  /* Find conflicts */
  findConflicts();

  /* Cleanup */
  free(g.inFreelist);
  free(g.inUse);
  close(g.fd);

  return 0;
}
