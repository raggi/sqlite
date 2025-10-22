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

/* Global state */
static struct {
  int fd;           /* File descriptor */
  u32 pagesize;     /* Page size from header */
  u32 mxPage;       /* Maximum page number */
  u32 firstFreelist; /* First freelist trunk page */
  u32 freelistCount; /* Freelist count from header */
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
  
  header = readBytes(0, 100);
  if( !header ) return 1;
  
  /* Check magic string */
  if( memcmp(header, "SQLite format 3\000", 16) != 0 ){
    fprintf(stderr, "ERROR: not a SQLite database file\n");
    free(header);
    return 1;
  }
  
  /* Extract page size */
  g.pagesize = (header[16]<<8) | (header[17]<<16);
  if( g.pagesize==0 ) g.pagesize = 65536;
  if( g.pagesize==1 ) g.pagesize = 65536;
  
  /* Get file size to calculate max page */
  fileSize = lseek(g.fd, 0, SEEK_END);
  if( fileSize < 0 ){
    fprintf(stderr, "ERROR: cannot determine file size\n");
    free(header);
    return 1;
  }
  g.mxPage = (u32)((fileSize + g.pagesize - 1) / g.pagesize);
  
  /* Extract freelist information */
  g.firstFreelist = get32(header + 32);
  g.freelistCount = get32(header + 36);
  
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
  u32 visited[10000];
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
    if( visitCount < 10000 ){
      visited[visitCount++] = pgno;
    }
    
    /* Read trunk page */
    page = readPage(pgno);
    if( !page ) return 1;
    
    trunkCount++;
    markFreelist(pgno);
    
    nextTrunk = get32(page);
    numLeaves = get32(page + 4);
    
    /* Sanity check */
    if( numLeaves > (g.pagesize - 8) / 4 ){
      fprintf(stderr, "ERROR: trunk page %u has invalid leaf count\n", pgno);
      numLeaves = (g.pagesize - 8) / 4;
    }
    
    /* Mark leaf pages */
    for(i=0; i<numLeaves; i++){
      u32 leafPgno = get32(page + 8 + i*4);
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
  if( depth > 50 ) return; /* Prevent deep recursion */
  
  markInUse(pgno);
  
  page = readPage(pgno);
  if( !page ) return;
  
  hdr = (pgno == 1) ? 100 : 0;
  pageType = page[hdr];
  
  /* Only handle btree pages */
  if( pageType != 2 && pageType != 5 && pageType != 10 && pageType != 13 ){
    free(page);
    return;
  }
  
  nCell = (page[hdr+3]<<8) | page[hdr+4];
  
  /* Interior table (2) or interior index (5) - walk child pointers */
  if( pageType == 2 || pageType == 5 ){
    cellStart = hdr + 12;
    
    /* Walk each cell's child pointer */
    for(i=0; i<nCell && i<g.pagesize/2; i++){
      u32 cellOffset = (page[cellStart + i*2]<<8) | page[cellStart + i*2 + 1];
      if( cellOffset >= 4 && cellOffset < g.pagesize ){
        u32 childPage = get32(page + cellOffset);
        walkBtree(childPage, depth + 1);
      }
    }
    
    /* Rightmost child */
    if( cellStart >= 4 ){
      u32 rightChild = get32(page + cellStart - 4);
      walkBtree(rightChild, depth + 1);
    }
  }
  
  /* Check for overflow pages in leaf cells */
  if( pageType == 13 || pageType == 10 ){
    cellStart = hdr + 8 + ((pageType <= 5) ? 4 : 0);
    
    for(i=0; i<nCell && i<g.pagesize/2; i++){
      u32 cellOffset = (page[cellStart + i*2]<<8) | page[cellStart + i*2 + 1];
      if( cellOffset < g.pagesize - 4 ){
        u8 *cell = page + cellOffset;
        i64 nPayload;
        int n;
        
        /* Skip rowid for table leaf */
        if( pageType == 13 ){
          n = decodeVarint(cell, &nPayload);
          cell += n;
        }
        
        /* Get payload size */
        n = decodeVarint(cell, &nPayload);
        cell += n;
        
        /* Check if there are overflow pages */
        u32 usable = g.pagesize - page[20];
        u32 maxLocal = usable - 35;
        if( pageType == 13 ){
          u32 minLocal = ((usable - 12) * 32 / 255) - 23;
          if( nPayload > maxLocal ){
            /* Has overflow - find first overflow page */
            u32 localSize = minLocal + ((nPayload - minLocal) % (usable - 4));
            if( (u32)(cell - page + localSize + 4) <= g.pagesize ){
              u32 overflowPage = get32(cell + localSize);
              /* Walk overflow chain */
              while( overflowPage > 0 && overflowPage <= g.mxPage ){
                markInUse(overflowPage);
                u8 *ovfl = readPage(overflowPage);
                if( !ovfl ) break;
                overflowPage = get32(ovfl);
                free(ovfl);
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