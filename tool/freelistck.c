/*
** A utility for checking SQLite database freelist integrity.
** 
** This tool walks the freelist chain and reports:
** - Total count of freelist pages (trunk + leaf)
** - Expected count from database header
** - Any discrepancies
** - All page numbers in the freelist
**
** Usage: freelistck DATABASE_FILE
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

typedef unsigned char u8;
typedef unsigned int u32;

/*
** Constants derived from SQLite file format specification
** Reference: https://www.sqlite.org/fileformat.html
*/

/* Database header offsets (Section 1.3) */
#define SQLITE_HEADER_SIZE           100   /* Size of database file header */
#define SQLITE_HEADER_MAGIC_OFFSET   0     /* Offset to magic string */
#define SQLITE_HEADER_MAGIC_SIZE     16    /* Size of magic string */
#define SQLITE_HEADER_PAGESIZE_OFFSET 16   /* Offset to page size (2 bytes) */
#define SQLITE_HEADER_FREELIST_OFFSET 32   /* Offset to first freelist trunk page */
#define SQLITE_HEADER_FREELIST_COUNT  36   /* Offset to total freelist page count */

/* Special page size values (Section 1.3.2) */
#define SQLITE_PAGESIZE_MAGIC_65536  1     /* Magic value representing 65536 bytes */
#define SQLITE_PAGESIZE_DEFAULT      1024  /* Default page size for value 0 */
#define SQLITE_PAGESIZE_MAX          65536 /* Maximum page size */

/* Freelist structure offsets (Section 1.5) */
#define FREELIST_TRUNK_NEXT_OFFSET   0     /* Next trunk page pointer (4 bytes) */
#define FREELIST_TRUNK_COUNT_OFFSET  4     /* Leaf page count (4 bytes) */
#define FREELIST_TRUNK_LEAVES_OFFSET 8     /* Start of leaf page array */
#define FREELIST_TRUNK_HEADER_SIZE   8     /* Size of trunk page header */
#define FREELIST_LEAF_ENTRY_SIZE     4     /* Size of each leaf page entry */

/* Cycle detection limit */
#define MAX_FREELIST_CYCLE_CHECK     10000 /* Maximum pages to track for cycles */

/* Formatting constants */
#define LEAF_PAGES_PER_LINE          8     /* Leaf pages to display per line */

/* Global state */
static struct {
  int fd;           /* File descriptor */
  u32 pagesize;     /* Page size from header */
  u32 mxPage;       /* Maximum page number */
  u32 firstFreelist; /* First freelist trunk page */
  u32 freelistCount; /* Freelist count from header */
} g;

/* Extract a big-endian 32-bit integer */
static u32 get32(const u8 *z){
  return (z[0]<<24) + (z[1]<<16) + (z[2]<<8) + z[3];
}

/* Read a page from the database file */
static u8* readPage(u32 pgno){
  u8 *buf;
  off_t offset;
  ssize_t n;
  
  if( pgno<1 || pgno>g.mxPage ){
    fprintf(stderr, "ERROR: page %u out of range 1..%u\n", pgno, g.mxPage);
    return NULL;
  }
  
  buf = malloc(g.pagesize);
  if( !buf ){
    fprintf(stderr, "ERROR: out of memory\n");
    return NULL;
  }
  
  offset = (off_t)(pgno-1) * g.pagesize;
  if( lseek(g.fd, offset, SEEK_SET) != offset ){
    fprintf(stderr, "ERROR: seek failed for page %u\n", pgno);
    free(buf);
    return NULL;
  }
  
  n = read(g.fd, buf, g.pagesize);
  if( n != (ssize_t)g.pagesize ){
    fprintf(stderr, "ERROR: read failed for page %u\n", pgno);
    free(buf);
    return NULL;
  }
  
  return buf;
}

/* Read and parse the database header */
static int readHeader(void){
  u8 header[100];
  ssize_t n;
  off_t fileSize;
  
  if( lseek(g.fd, 0, SEEK_SET) != 0 ){
    fprintf(stderr, "ERROR: seek to beginning failed\n");
    return 1;
  }
  
  n = read(g.fd, header, SQLITE_HEADER_SIZE);
  if( n != SQLITE_HEADER_SIZE ){
    fprintf(stderr, "ERROR: cannot read database header\n");
    return 1;
  }
  
  /* Check magic string "SQLite format 3\0" (Section 1.3.1) */
  if( memcmp(header + SQLITE_HEADER_MAGIC_OFFSET, 
             "SQLite format 3\000", SQLITE_HEADER_MAGIC_SIZE) != 0 ){
    fprintf(stderr, "ERROR: not a SQLite database file\n");
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
    return 1;
  }
  g.mxPage = (u32)((fileSize + g.pagesize - 1) / g.pagesize);
  
  /* Extract freelist information (Section 1.3.8) */
  g.firstFreelist = get32(header + SQLITE_HEADER_FREELIST_OFFSET);
  g.freelistCount = get32(header + SQLITE_HEADER_FREELIST_COUNT);
  
  return 0;
}

/* Structure to store freelist page info */
typedef struct FreelistPage FreelistPage;
struct FreelistPage {
  u32 pgno;              /* Page number */
  int isTrunk;           /* 1 if trunk, 0 if leaf */
  u32 parent;            /* Parent trunk page (0 if this is trunk) */
  FreelistPage *next;
};

static FreelistPage *freelistHead = NULL;
static FreelistPage *freelistTail = NULL;
static u32 trunkCount = 0;
static u32 leafCount = 0;

/* Add a page to the freelist tracking */
static void addFreelistPage(u32 pgno, int isTrunk, u32 parent){
  FreelistPage *p = malloc(sizeof(*p));
  if( !p ){
    fprintf(stderr, "ERROR: out of memory\n");
    exit(1);
  }
  p->pgno = pgno;
  p->isTrunk = isTrunk;
  p->parent = parent;
  p->next = NULL;
  
  if( freelistTail ){
    freelistTail->next = p;
  }else{
    freelistHead = p;
  }
  freelistTail = p;
  
  if( isTrunk ){
    trunkCount++;
  }else{
    leafCount++;
  }
}

/* Walk the freelist chain */
static int walkFreelist(void){
  u32 pgno = g.firstFreelist;
  u32 trunkNum = 0;
  u32 visited[MAX_FREELIST_CYCLE_CHECK]; /* Guard against cycles */
  u32 visitCount = 0;
  
  printf("Walking freelist starting at page %u...\n", pgno);
  printf("\n");
  
  while( pgno != 0 ){
    u8 *page;
    u32 nextTrunk;
    u32 numLeaves;
    u32 i;
    
    /* Check for cycles */
    for(i=0; i<visitCount; i++){
      if( visited[i] == pgno ){
        fprintf(stderr, "ERROR: cycle detected in freelist at page %u\n", pgno);
        return 1;
      }
    }
    if( visitCount < MAX_FREELIST_CYCLE_CHECK ){
      visited[visitCount++] = pgno;
    }
    
    /* Read trunk page */
    page = readPage(pgno);
    if( !page ) return 1;
    
    trunkNum++;
    addFreelistPage(pgno, 1, 0);
    
    /* Read trunk page structure (Section 1.5) */
    nextTrunk = get32(page + FREELIST_TRUNK_NEXT_OFFSET);
    numLeaves = get32(page + FREELIST_TRUNK_COUNT_OFFSET);
    
    printf("Trunk page %u (trunk #%u):\n", pgno, trunkNum);
    printf("  Next trunk: %u\n", nextTrunk);
    printf("  Leaf count: %u\n", numLeaves);
    
    /* Sanity check leaf count - max entries that fit after 8-byte header */
    u32 maxLeaves = (g.pagesize - FREELIST_TRUNK_HEADER_SIZE) / FREELIST_LEAF_ENTRY_SIZE;
    if( numLeaves > maxLeaves ){
      fprintf(stderr, "ERROR: trunk page %u has invalid leaf count %u (max %u)\n",
              pgno, numLeaves, maxLeaves);
      numLeaves = maxLeaves;
    }
    
    /* Process leaf pages */
    if( numLeaves > 0 ){
      printf("  Leaf pages:");
      for(i=0; i<numLeaves; i++){
        u32 leafPgno = get32(page + FREELIST_TRUNK_LEAVES_OFFSET + i*FREELIST_LEAF_ENTRY_SIZE);
        if( i % LEAF_PAGES_PER_LINE == 0 ){
          printf("\n    ");
        }
        printf("%u ", leafPgno);
        addFreelistPage(leafPgno, 0, pgno);
      }
      printf("\n");
    }
    printf("\n");
    
    free(page);
    pgno = nextTrunk;
  }
  
  return 0;
}

/* Print summary */
static void printSummary(void){
  u32 totalPages = trunkCount + leafCount;
  
  printf("=== FREELIST SUMMARY ===\n");
  printf("Trunk pages: %u\n", trunkCount);
  printf("Leaf pages:  %u\n", leafCount);
  printf("Total:       %u\n", totalPages);
  printf("\n");
  printf("Header says: %u freelist pages\n", g.freelistCount);
  printf("\n");
  
  if( totalPages == g.freelistCount ){
    printf("✓ Freelist count matches header\n");
  }else{
    printf("✗ MISMATCH: Found %u pages but header says %u\n", 
           totalPages, g.freelistCount);
    printf("  Difference: %d pages\n", (int)totalPages - (int)g.freelistCount);
    
    if( totalPages > g.freelistCount ){
      printf("\n");
      printf("This suggests that the freelist chain contains %u extra page(s)\n",
             totalPages - g.freelistCount);
      printf("that should have been removed when they were allocated.\n");
    }else{
      printf("\n");
      printf("This suggests that the header count is too high,\n");
      printf("or some freelist pages are not reachable via the chain.\n");
    }
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
  
  /* Read and parse header */
  if( readHeader() ){
    close(g.fd);
    return 1;
  }
  
  printf("Database: %s\n", argv[1]);
  printf("Page size: %u bytes\n", g.pagesize);
  printf("Total pages: %u\n", g.mxPage);
  printf("First freelist trunk: %u\n", g.firstFreelist);
  printf("Freelist count (from header): %u\n", g.freelistCount);
  printf("\n");
  
  /* Walk the freelist */
  if( g.firstFreelist == 0 ){
    printf("Freelist is empty.\n");
  }else{
    if( walkFreelist() ){
      close(g.fd);
      return 1;
    }
  }
  
  /* Print summary */
  printSummary();
  
  close(g.fd);
  return 0;
}