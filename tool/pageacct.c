/*
** A utility for comprehensive SQLite database page accounting.
**
** This tool provides a complete accounting of every page in the database:
** - Pages in freelist (trunk and leaf)
** - Pages in btrees (interior and leaf)
** - Overflow pages
** - Lock-byte page
** - Pointer map pages (if auto-vacuum enabled)
** - UNACCOUNTED pages (the mystery we're looking for!)
**
** Usage: pageacct DATABASE_FILE
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
** Constants from SQLite file format
*/

/* Database header offsets */
#define SQLITE_HEADER_SIZE           100
#define SQLITE_HEADER_MAGIC_OFFSET   0
#define SQLITE_HEADER_MAGIC_SIZE     16
#define SQLITE_HEADER_PAGESIZE_OFFSET 16
#define SQLITE_HEADER_RESERVED_OFFSET 20
#define SQLITE_HEADER_DBSIZE_OFFSET   28
#define SQLITE_HEADER_FREELIST_OFFSET 32
#define SQLITE_HEADER_FREELIST_COUNT  36
#define SQLITE_HEADER_AUTOVACUUM_OFFSET 52

/* Page size constants */
#define SQLITE_PAGESIZE_MAGIC_65536  1
#define SQLITE_PAGESIZE_DEFAULT      1024
#define SQLITE_PAGESIZE_MAX          65536

/* Page 1 special offset */
#define PAGE1_HEADER_OFFSET          100

/* B-tree page types */
#define BTREE_INTERIOR_INDEX         2
#define BTREE_INTERIOR_TABLE         5
#define BTREE_LEAF_INDEX             10
#define BTREE_LEAF_TABLE             13

/* B-tree header offsets */
#define BTREE_HEADER_PAGETYPE        0
#define BTREE_HEADER_FREEBLOCK       1
#define BTREE_HEADER_NCELLS          3
#define BTREE_HEADER_CELL_OFFSET     5
#define BTREE_HEADER_NFRAGMENTS      7
#define BTREE_HEADER_RIGHTCHILD      8
#define BTREE_HEADER_SIZE_INTERIOR   12
#define BTREE_HEADER_SIZE_LEAF       8

/* Freelist structure */
#define FREELIST_TRUNK_NEXT_OFFSET   0
#define FREELIST_TRUNK_COUNT_OFFSET  4
#define FREELIST_TRUNK_LEAVES_OFFSET 8
#define FREELIST_TRUNK_HEADER_SIZE   8
#define FREELIST_LEAF_ENTRY_SIZE     4

/* Overflow pages */
#define OVERFLOW_NEXT_OFFSET         0
#define OVERFLOW_HEADER_SIZE         4

/* Cell and payload constants */
#define CELL_POINTER_SIZE            2
#define CHILD_POINTER_SIZE           4
#define PAYLOAD_MIN_FRACTION         32
#define PAYLOAD_DIVISOR              255
#define PAYLOAD_MIN_SUBTRACT         23
#define PAYLOAD_MAX_SUBTRACT         35
#define PAYLOAD_USABLE_SUBTRACT      12

/* Safety limits */
#define MAX_BTREE_DEPTH              50
#define MAX_FREELIST_CYCLE_CHECK     10000

/* Page type classification */
typedef enum {
  PAGE_UNKNOWN = 0,
  PAGE_FREELIST_TRUNK,
  PAGE_FREELIST_LEAF,
  PAGE_BTREE_INTERIOR_INDEX,
  PAGE_BTREE_INTERIOR_TABLE,
  PAGE_BTREE_LEAF_INDEX,
  PAGE_BTREE_LEAF_TABLE,
  PAGE_OVERFLOW,
  PAGE_PTRMAP,
  PAGE_LOCKBYTE,
  /* Orphaned page types (unaccounted) */
  PAGE_ORPHAN_BTREE_INTERIOR_INDEX,
  PAGE_ORPHAN_BTREE_INTERIOR_TABLE,
  PAGE_ORPHAN_BTREE_LEAF_INDEX,
  PAGE_ORPHAN_BTREE_LEAF_TABLE,
  PAGE_ORPHAN_OVERFLOW,
  PAGE_ORPHAN_EMPTY
} PageType;

/* Global state */
static struct {
  int fd;
  u32 pagesize;
  u32 mxPage;
  u32 headerPageCount;  /* Page count from database header */
  u32 firstFreelist;
  u32 freelistCount;
  u32 reservedSpace;
  u32 autoVacuum;       /* Auto-vacuum mode from header */
  PageType *pageTypes;  /* What type is each page? */
  u32 *pageParents;     /* Which page references this page? */
  u32 ptrmapGhostCount; /* Pages at ptrmap positions with ptrmap data when autovacuum=0 */
  u32 ptrmapMissingCount; /* Pages at ptrmap positions without ptrmap data when autovacuum!=0 */
  u32 orphanCount;      /* Total orphaned pages */
} g;

/* Page type names for display */
static const char* pageTypeName(PageType type){
  switch(type){
    case PAGE_FREELIST_TRUNK: return "Freelist Trunk";
    case PAGE_FREELIST_LEAF: return "Freelist Leaf";
    case PAGE_BTREE_INTERIOR_INDEX: return "Btree Interior Index";
    case PAGE_BTREE_INTERIOR_TABLE: return "Btree Interior Table";
    case PAGE_BTREE_LEAF_INDEX: return "Btree Leaf Index";
    case PAGE_BTREE_LEAF_TABLE: return "Btree Leaf Table";
    case PAGE_OVERFLOW: return "Overflow";
    case PAGE_PTRMAP: return "Pointer Map";
    case PAGE_LOCKBYTE: return "Lock-byte";
    case PAGE_ORPHAN_BTREE_INTERIOR_INDEX: return "Orphan Btree Interior Index";
    case PAGE_ORPHAN_BTREE_INTERIOR_TABLE: return "Orphan Btree Interior Table";
    case PAGE_ORPHAN_BTREE_LEAF_INDEX: return "Orphan Btree Leaf Index";
    case PAGE_ORPHAN_BTREE_LEAF_TABLE: return "Orphan Btree Leaf Table";
    case PAGE_ORPHAN_OVERFLOW: return "Orphan Overflow";
    case PAGE_ORPHAN_EMPTY: return "Orphan Empty";
    default: return "Unknown";
  }
}

/* Extract big-endian 32-bit integer */
static u32 get32(const u8 *z){
  return (z[0]<<24) + (z[1]<<16) + (z[2]<<8) + z[3];
}

/* Read bytes from file */
static u8* readBytes(off_t offset, size_t size){
  u8 *buf = malloc(size);
  ssize_t n;

  if(!buf) return NULL;

  if(lseek(g.fd, offset, SEEK_SET) != offset){
    free(buf);
    return NULL;
  }

  n = read(g.fd, buf, size);
  if(n != (ssize_t)size){
    free(buf);
    return NULL;
  }

  return buf;
}

/* Read a page */
static u8* readPage(u32 pgno){
  if(pgno < 1 || pgno > g.mxPage) return NULL;
  return readBytes((off_t)(pgno-1) * g.pagesize, g.pagesize);
}

/* Read and parse database header */
static int readHeader(void){
  u8 *header;
  off_t fileSize;

  header = readBytes(0, SQLITE_HEADER_SIZE);
  if(!header) return 1;

  /* Check magic string */
  if(memcmp(header + SQLITE_HEADER_MAGIC_OFFSET,
            "SQLite format 3\000", SQLITE_HEADER_MAGIC_SIZE) != 0){
    fprintf(stderr, "ERROR: not a SQLite database file\n");
    free(header);
    return 1;
  }

  /* Extract page size */
  g.pagesize = (header[SQLITE_HEADER_PAGESIZE_OFFSET]<<8) |
                header[SQLITE_HEADER_PAGESIZE_OFFSET+1];
  if(g.pagesize == SQLITE_PAGESIZE_MAGIC_65536){
    g.pagesize = SQLITE_PAGESIZE_MAX;
  }
  if(g.pagesize == 0){
    g.pagesize = SQLITE_PAGESIZE_DEFAULT;
  }

  /* Extract database size from header */
  g.headerPageCount = get32(header + SQLITE_HEADER_DBSIZE_OFFSET);

  /* Get file size */
  fileSize = lseek(g.fd, 0, SEEK_END);
  if(fileSize < 0){
    free(header);
    return 1;
  }
  g.mxPage = (u32)((fileSize + g.pagesize - 1) / g.pagesize);

  /* Extract freelist info */
  g.firstFreelist = get32(header + SQLITE_HEADER_FREELIST_OFFSET);
  g.freelistCount = get32(header + SQLITE_HEADER_FREELIST_COUNT);

  /* Extract reserved space */
  g.reservedSpace = header[SQLITE_HEADER_RESERVED_OFFSET];

  /* Extract auto-vacuum mode */
  g.autoVacuum = get32(header + SQLITE_HEADER_AUTOVACUUM_OFFSET);

  free(header);
  return 0;
}

/* Mark a page with its type and parent */
static void markPage(u32 pgno, PageType type, u32 parent){
  if(pgno < 1 || pgno > g.mxPage) return;

  /* If already marked, detect conflicts */
  if(g.pageTypes[pgno] != PAGE_UNKNOWN && g.pageTypes[pgno] != type){
    printf("⚠️  CONFLICT: Page %u marked as both %s (parent %u) and %s (parent %u)\n",
           pgno, pageTypeName(g.pageTypes[pgno]), g.pageParents[pgno],
           pageTypeName(type), parent);
  }

  g.pageTypes[pgno] = type;
  g.pageParents[pgno] = parent;
}

/* Walk freelist chain */
static int walkFreelist(void){
  u32 pgno = g.firstFreelist;
  u32 visited[MAX_FREELIST_CYCLE_CHECK];
  u32 visitCount = 0;

  while(pgno != 0){
    u8 *page;
    u32 nextTrunk;
    u32 numLeaves;
    u32 i;

    /* Check for cycles */
    for(i=0; i<visitCount; i++){
      if(visited[i] == pgno){
        fprintf(stderr, "ERROR: cycle in freelist at page %u\n", pgno);
        return 1;
      }
    }
    if(visitCount < MAX_FREELIST_CYCLE_CHECK){
      visited[visitCount++] = pgno;
    }

    /* Read trunk page */
    page = readPage(pgno);
    if(!page) return 1;

    markPage(pgno, PAGE_FREELIST_TRUNK, 0);

    /* Read trunk structure */
    nextTrunk = get32(page + FREELIST_TRUNK_NEXT_OFFSET);
    numLeaves = get32(page + FREELIST_TRUNK_COUNT_OFFSET);

    /* Sanity check */
    u32 maxLeaves = (g.pagesize - FREELIST_TRUNK_HEADER_SIZE) / FREELIST_LEAF_ENTRY_SIZE;
    if(numLeaves > maxLeaves){
      numLeaves = maxLeaves;
    }

    /* Mark leaf pages */
    for(i=0; i<numLeaves; i++){
      u32 leafPgno = get32(page + FREELIST_TRUNK_LEAVES_OFFSET + i*FREELIST_LEAF_ENTRY_SIZE);
      markPage(leafPgno, PAGE_FREELIST_LEAF, pgno);
    }

    free(page);
    pgno = nextTrunk;
  }

  return 0;
}

/* Decode varint */
static int decodeVarint(const u8 *z, i64 *pVal){
  i64 v = 0;
  int i;
  for(i=0; i<8; i++){
    v = (v<<7) + (z[i]&0x7f);
    if((z[i]&0x80)==0){ *pVal = v; return i+1; }
  }
  v = (v<<8) + (z[i]&0xff);
  *pVal = v;
  return 9;
}

/* Forward declaration */
static int isValidPtrmapData(u8 *page);

/* Check if a page number is at a ptrmap position */
static int isPtrmapPosition(u32 pgno){
  u32 usableSize = g.pagesize - g.reservedSpace;
  u32 pagesPerPtrmap = usableSize / 5;  /* 5 bytes per entry */
  u32 firstPtrmap = pagesPerPtrmap + 1;

  if(pgno == 1) return 0;
  if(pgno < firstPtrmap) return 0;

  u32 offset = pgno - firstPtrmap;
  if(offset % (pagesPerPtrmap + 1) == 0){
    return 1;
  }

  return 0;
}

/* Check if a page number should be a ptrmap page */
static int isPtrmapPage(u32 pgno){
  /* No ptrmap pages if auto-vacuum is disabled */
  if(g.autoVacuum == 0) return 0;

  return isPtrmapPosition(pgno);
}

/* Walk btree recursively */
static void walkBtree(u32 pgno, u32 parent, int depth){
  u8 *page;
  int hdr;
  u8 pageType;
  u32 nCell;
  u32 i;
  int cellStart;
  PageType ourType;

  if(pgno < 1 || pgno > g.mxPage) return;
  if(g.pageTypes[pgno] != PAGE_UNKNOWN) return; /* Already visited */
  if(depth > MAX_BTREE_DEPTH) return;

  /* Skip ptrmap pages - they're not part of btree */
  if(isPtrmapPage(pgno)) return;

  /* Check if page is at ptrmap position but autovacuum is disabled */
  if(g.autoVacuum == 0 && isPtrmapPosition(pgno)){
    /* Read the page to check if it contains ptrmap data */
    u8 *ptrmapCheck = readPage(pgno);
    if(ptrmapCheck && isValidPtrmapData(ptrmapCheck)){
      /* This looks like a ghost ptrmap page - count it but continue walking */
      g.ptrmapGhostCount++;
      free(ptrmapCheck);
      /* Don't return - continue to process as normal page */
    } else {
      if(ptrmapCheck) free(ptrmapCheck);
    }
  }

  page = readPage(pgno);
  if(!page) return;

  /* Page 1 has database header */
  hdr = (pgno == 1) ? PAGE1_HEADER_OFFSET : 0;
  pageType = page[hdr + BTREE_HEADER_PAGETYPE];

  /* Map page type byte to our enum */
  switch(pageType){
    case BTREE_INTERIOR_INDEX: ourType = PAGE_BTREE_INTERIOR_INDEX; break;
    case BTREE_INTERIOR_TABLE: ourType = PAGE_BTREE_INTERIOR_TABLE; break;
    case BTREE_LEAF_INDEX: ourType = PAGE_BTREE_LEAF_INDEX; break;
    case BTREE_LEAF_TABLE: ourType = PAGE_BTREE_LEAF_TABLE; break;
    default:
      free(page);
      return; /* Not a btree page */
  }

  markPage(pgno, ourType, parent);

  /* Read number of cells */
  nCell = (page[hdr + BTREE_HEADER_NCELLS]<<8) | page[hdr + BTREE_HEADER_NCELLS + 1];

  /* Handle interior pages - walk child pointers */
  if(pageType == BTREE_INTERIOR_INDEX || pageType == BTREE_INTERIOR_TABLE){
    cellStart = hdr + BTREE_HEADER_SIZE_INTERIOR;

    /* Walk each cell's left child */
    for(i=0; i<nCell && i<g.pagesize/CELL_POINTER_SIZE; i++){
      u32 cellOffset = (page[cellStart + i*CELL_POINTER_SIZE]<<8) |
                        page[cellStart + i*CELL_POINTER_SIZE + 1];
      if(cellOffset >= CHILD_POINTER_SIZE && cellOffset < g.pagesize){
        u32 childPage = get32(page + cellOffset);
        walkBtree(childPage, pgno, depth + 1);
      }
    }

    /* Rightmost child */
    u32 rightChild = get32(page + hdr + BTREE_HEADER_RIGHTCHILD);
    walkBtree(rightChild, pgno, depth + 1);
  }

  /* Handle interior index pages - check for overflow */
  if(pageType == BTREE_INTERIOR_INDEX){
    cellStart = hdr + BTREE_HEADER_SIZE_INTERIOR;

    for(i=0; i<nCell && i<g.pagesize/CELL_POINTER_SIZE; i++){
      u32 cellOffset = (page[cellStart + i*CELL_POINTER_SIZE]<<8) |
                        page[cellStart + i*CELL_POINTER_SIZE + 1];
      if(cellOffset < g.pagesize - CHILD_POINTER_SIZE - CHILD_POINTER_SIZE){
        u8 *cell = page + cellOffset;
        i64 nPayload;
        int n;

        /* Skip child pointer (4 bytes) */
        cell += CHILD_POINTER_SIZE;

        /* Get payload size */
        n = decodeVarint(cell, &nPayload);
        cell += n;

        /* Check for overflow pages */
        if(nPayload > 0 && nPayload < 1073741824){
          u32 usable = g.pagesize - g.reservedSpace;
          u32 maxLocal;
          u32 minLocal;
          u32 localSize;

          /* Interior index cell overflow calculation */
          maxLocal = ((usable - 12) * 64 / 255) - 23;
          minLocal = ((usable - 12) * 32 / 255) - 23;

          if(nPayload > maxLocal){
            u32 surplus = minLocal + ((nPayload - minLocal) % (usable - OVERFLOW_HEADER_SIZE));
            if(surplus <= maxLocal){
              localSize = surplus;
            }else{
              localSize = minLocal;
            }
            if((u32)(cell - page + localSize + CHILD_POINTER_SIZE) <= g.pagesize){
              u32 overflowPage = get32(cell + localSize);

              /* Walk overflow chain */
              while(overflowPage > 0 && overflowPage <= g.mxPage){
                if(g.pageTypes[overflowPage] != PAGE_UNKNOWN) break;
                markPage(overflowPage, PAGE_OVERFLOW, pgno);

                u8 *ovfl = readPage(overflowPage);
                if(!ovfl) break;
                overflowPage = get32(ovfl + OVERFLOW_NEXT_OFFSET);
                free(ovfl);
              }
            }
          }
        }
      }
    }
  }

  /* Handle leaf pages - check for overflow */
  if(pageType == BTREE_LEAF_TABLE || pageType == BTREE_LEAF_INDEX){
    cellStart = hdr + BTREE_HEADER_SIZE_LEAF;

    for(i=0; i<nCell && i<g.pagesize/CELL_POINTER_SIZE; i++){
      u32 cellOffset = (page[cellStart + i*CELL_POINTER_SIZE]<<8) |
                        page[cellStart + i*CELL_POINTER_SIZE + 1];
      if(cellOffset < g.pagesize - CHILD_POINTER_SIZE){
        u8 *cell = page + cellOffset;
        i64 nPayload;
        i64 rowid;
        int n;

        /* Get payload size */
        n = decodeVarint(cell, &nPayload);
        cell += n;

        /* Skip rowid for table leaf */
        if(pageType == BTREE_LEAF_TABLE){
          n = decodeVarint(cell, &rowid);
          cell += n;
        }

        /* Check for overflow pages */
        if(nPayload > 0 && nPayload < 1073741824){
          u32 usable = g.pagesize - g.reservedSpace;
          u32 maxLocal;
          u32 minLocal;
          u32 localSize;

          if(pageType == BTREE_LEAF_TABLE){
            /* Table b-tree leaf cell overflow calculation */
            maxLocal = usable - PAYLOAD_MAX_SUBTRACT;
            minLocal = ((usable - PAYLOAD_USABLE_SUBTRACT) * PAYLOAD_MIN_FRACTION
                        / PAYLOAD_DIVISOR) - PAYLOAD_MIN_SUBTRACT;

            if(nPayload > maxLocal){
              u32 surplus = minLocal + ((nPayload - minLocal) % (usable - OVERFLOW_HEADER_SIZE));
              if(surplus <= maxLocal){
                localSize = surplus;
              }else{
                localSize = minLocal;
              }
              if((u32)(cell - page + localSize + CHILD_POINTER_SIZE) <= g.pagesize){
                u32 overflowPage = get32(cell + localSize);

                /* Walk overflow chain */
                while(overflowPage > 0 && overflowPage <= g.mxPage){
                  if(g.pageTypes[overflowPage] != PAGE_UNKNOWN) break;
                  markPage(overflowPage, PAGE_OVERFLOW, pgno);

                  u8 *ovfl = readPage(overflowPage);
                  if(!ovfl) break;
                  overflowPage = get32(ovfl + OVERFLOW_NEXT_OFFSET);
                  free(ovfl);
                }
              }
            }
          } else if(pageType == BTREE_LEAF_INDEX){
            /* Index b-tree leaf cell overflow calculation */
            /* For index leaves: maxLocal = ((U-12)*64/255)-23 */
            maxLocal = ((usable - 12) * 64 / 255) - 23;
            minLocal = ((usable - 12) * 32 / 255) - 23;

            if(nPayload > maxLocal){
              u32 surplus = minLocal + ((nPayload - minLocal) % (usable - OVERFLOW_HEADER_SIZE));
              if(surplus <= maxLocal){
                localSize = surplus;
              }else{
                localSize = minLocal;
              }
              
              if((u32)(cell - page + localSize + CHILD_POINTER_SIZE) <= g.pagesize){
                u32 overflowPage = get32(cell + localSize);

                /* Walk overflow chain */
                while(overflowPage > 0 && overflowPage <= g.mxPage){
                  if(g.pageTypes[overflowPage] != PAGE_UNKNOWN) break;
                  markPage(overflowPage, PAGE_OVERFLOW, pgno);

                  u8 *ovfl = readPage(overflowPage);
                  if(!ovfl) break;
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

/* Walk all btrees */
static int walkAllBtrees(const char *dbPath){
  sqlite3 *db;
  sqlite3_stmt *stmt;
  int rc;

  /* Mark page 1 */
  walkBtree(1, 0, 0);

  /* Open database to read schema */
  rc = sqlite3_open_v2(dbPath, &db, SQLITE_OPEN_READONLY, NULL);
  if(rc != SQLITE_OK){
    fprintf(stderr, "ERROR: cannot open database: %s\n", sqlite3_errmsg(db));
    sqlite3_close(db);
    return 1;
  }

  /* Query all root pages */
  rc = sqlite3_prepare_v2(db,
    "SELECT name, rootpage FROM sqlite_master WHERE rootpage > 0",
    -1, &stmt, NULL);

  if(rc != SQLITE_OK){
    fprintf(stderr, "ERROR: cannot query schema: %s\n", sqlite3_errmsg(db));
    sqlite3_close(db);
    return 1;
  }

  while(sqlite3_step(stmt) == SQLITE_ROW){
    u32 rootpage = (u32)sqlite3_column_int(stmt, 1);
    walkBtree(rootpage, 0, 0);
  }

  sqlite3_finalize(stmt);
  sqlite3_close(db);

  return 0;
}

/* Mark pointer map pages */
/*
** Check if a page contains valid pointer map data.
** Returns 1 if the page looks like a valid ptrmap page, 0 otherwise.
*/
static int isValidPtrmapData(u8 *page){
  u32 usableSize = g.pagesize - g.reservedSpace;
  u32 entriesPerPage = usableSize / 5;
  u32 i;
  int hasValidEntry = 0;

  /* Check each 5-byte entry */
  for(i = 0; i < entriesPerPage; i++){
    u8 type = page[i * 5];
    u32 parent = get32(page + i * 5 + 1);

    /* Type must be valid (1-5) or 0 (unused) */
    if(type > 5){
      return 0;  /* Invalid type byte */
    }

    /* If type is non-zero, validate parent page */
    if(type != 0){
      hasValidEntry = 1;
      /* Parent must be a valid page number (or 0 for root pages) */
      if(parent > g.mxPage){
        return 0;  /* Invalid parent reference */
      }
    }
  }

  /* A valid ptrmap should have at least some non-zero entries */
  return hasValidEntry;
}

/*
** Check pages at ptrmap positions and mark them if they contain valid ptrmap data.
*/
static void markPtrmapPages(void){
  u32 usableSize = g.pagesize - g.reservedSpace;
  u32 pagesPerPtrmap = usableSize / 5;
  u32 firstPtrmap = pagesPerPtrmap + 1;

  g.ptrmapGhostCount = 0;
  g.ptrmapMissingCount = 0;

  for(u32 pgno = firstPtrmap; pgno <= g.mxPage; pgno += (pagesPerPtrmap + 1)){
    /* Skip if this page is already classified (e.g., in freelist or btree) */
    if(g.pageTypes[pgno] != PAGE_UNKNOWN){
      if(g.autoVacuum != 0){
        /* Autovacuum enabled but ptrmap position is used for something else */
        g.ptrmapMissingCount++;
      }
      continue;
    }

    /* Read the page and check if it contains valid ptrmap data */
    u8 *page = readPage(pgno);
    if(!page){
      continue;
    }

    int isValid = isValidPtrmapData(page);
    free(page);

    if(isValid){
      /* Found valid ptrmap data */
      markPage(pgno, PAGE_PTRMAP, 0);

      if(g.autoVacuum == 0){
        /* Ghost ptrmap: autovacuum disabled but ptrmap page exists */
        g.ptrmapGhostCount++;
      }
    } else {
      if(g.autoVacuum != 0){
        /* Autovacuum enabled but ptrmap position doesn't have valid data */
        g.ptrmapMissingCount++;
      }
    }
  }
}

/*
** Check if a page looks like an overflow page
*/
static int isAllZeros(const u8 *data, u32 len){
  for(u32 i = 0; i < len; i++){
    if(data[i] != 0) return 0;
  }
  return 1;
}

/*
** Classify orphaned pages by their content
*/
static void classifyOrphanedPages(void){
  u32 i;
  g.orphanCount = 0;

  for(i = 1; i <= g.mxPage; i++){
    if(g.pageTypes[i] != PAGE_UNKNOWN) continue;

    /* Read the page to determine what type of orphan it is */
    u8 *page = readPage(i);
    if(!page) continue;

    /* Check if empty */
    if(isAllZeros(page, g.pagesize)){
      g.pageTypes[i] = PAGE_ORPHAN_EMPTY;
      g.orphanCount++;
      free(page);
      continue;
    }

    /* Check page type byte */
    u8 pageType = page[0];
    u32 nextPage = get32(page);

    if(pageType == 0x0d){
      g.pageTypes[i] = PAGE_ORPHAN_BTREE_LEAF_TABLE;
      g.orphanCount++;
    } else if(pageType == 0x0a){
      g.pageTypes[i] = PAGE_ORPHAN_BTREE_LEAF_INDEX;
      g.orphanCount++;
    } else if(pageType == 0x05){
      g.pageTypes[i] = PAGE_ORPHAN_BTREE_INTERIOR_TABLE;
      g.orphanCount++;
    } else if(pageType == 0x02){
      g.pageTypes[i] = PAGE_ORPHAN_BTREE_INTERIOR_INDEX;
      g.orphanCount++;
    } else if(pageType == 0x00 && (nextPage == 0 || (nextPage > 0 && nextPage < g.mxPage))){
      /* Looks like overflow page */
      g.pageTypes[i] = PAGE_ORPHAN_OVERFLOW;
      g.orphanCount++;
    } else {
      /* Leave as PAGE_UNKNOWN */
    }

    free(page);
  }
}

/* Print page accounting report */
static void printReport(void){
  u32 i;
  u32 counts[30] = {0}; /* Count by type */
  u32 *unknownPages = NULL; /* Store all unknown pages */
  u32 numUnknown = 0;
  u32 unknownCapacity = 1000;
  u32 *orphanedPages = NULL; /* Store all orphaned pages */
  u32 numOrphaned = 0;
  u32 orphanedCapacity = 1000;

  /* Allocate array for unknown pages */
  unknownPages = malloc(unknownCapacity * sizeof(u32));
  if(!unknownPages){
    fprintf(stderr, "ERROR: out of memory\n");
    return;
  }

  /* Allocate array for orphaned pages */
  orphanedPages = malloc(orphanedCapacity * sizeof(u32));
  if(!orphanedPages){
    fprintf(stderr, "ERROR: out of memory\n");
    free(unknownPages);
    return;
  }

  /* Count pages by type */
  for(i=1; i<=g.mxPage; i++){
    PageType type = g.pageTypes[i];
    if(type < 30){
      counts[type]++;
    }
    if(type == PAGE_UNKNOWN){
      if(numUnknown >= unknownCapacity){
        unknownCapacity *= 2;
        u32 *newPages = realloc(unknownPages, unknownCapacity * sizeof(u32));
        if(!newPages){
          fprintf(stderr, "ERROR: out of memory\n");
          free(unknownPages);
          free(orphanedPages);
          return;
        }
        unknownPages = newPages;
      }
      unknownPages[numUnknown++] = i;
    }
    /* Check if page is orphaned */
    if(type == PAGE_ORPHAN_BTREE_INTERIOR_INDEX ||
       type == PAGE_ORPHAN_BTREE_INTERIOR_TABLE ||
       type == PAGE_ORPHAN_BTREE_LEAF_INDEX ||
       type == PAGE_ORPHAN_BTREE_LEAF_TABLE ||
       type == PAGE_ORPHAN_OVERFLOW ||
       type == PAGE_ORPHAN_EMPTY){
      if(numOrphaned >= orphanedCapacity){
        orphanedCapacity *= 2;
        u32 *newPages = realloc(orphanedPages, orphanedCapacity * sizeof(u32));
        if(!newPages){
          fprintf(stderr, "ERROR: out of memory\n");
          free(unknownPages);
          free(orphanedPages);
          return;
        }
        orphanedPages = newPages;
      }
      orphanedPages[numOrphaned++] = i;
    }
  }

  printf("\n=== PAGE ACCOUNTING REPORT ===\n\n");

  printf("Database settings:\n");
  printf("  Page size:             %u bytes\n", g.pagesize);
  printf("  Auto-vacuum mode:      %u ", g.autoVacuum);
  switch(g.autoVacuum){
    case 0: printf("(NONE)\n"); break;
    case 1: printf("(FULL)\n"); break;
    case 2: printf("(INCREMENTAL)\n"); break;
    default: printf("(UNKNOWN)\n"); break;
  }
  printf("\n");

  printf("Page counts:\n");
  printf("  Header says:           %u pages\n", g.headerPageCount);
  printf("  File size calculates:  %u pages\n", g.mxPage);
  if(g.headerPageCount != g.mxPage){
    printf("  ⚠️  MISMATCH: %+d pages\n", (int)g.mxPage - (int)g.headerPageCount);
  }
  printf("\n");

  printf("Page counts by type:\n");
  printf("  Freelist Trunk:        %5u\n", counts[PAGE_FREELIST_TRUNK]);
  printf("  Freelist Leaf:         %5u\n", counts[PAGE_FREELIST_LEAF]);
  printf("  Btree Interior Index:  %5u\n", counts[PAGE_BTREE_INTERIOR_INDEX]);
  printf("  Btree Interior Table:  %5u\n", counts[PAGE_BTREE_INTERIOR_TABLE]);
  printf("  Btree Leaf Index:      %5u\n", counts[PAGE_BTREE_LEAF_INDEX]);
  printf("  Btree Leaf Table:      %5u\n", counts[PAGE_BTREE_LEAF_TABLE]);
  printf("  Overflow:              %5u\n", counts[PAGE_OVERFLOW]);
  printf("  Pointer Map:           %5u\n", counts[PAGE_PTRMAP]);
  printf("  Lock-byte:             %5u\n", counts[PAGE_LOCKBYTE]);
  printf("\n");

  /* Orphaned pages breakdown */
  u32 orphanBtreeLeafTable = counts[PAGE_ORPHAN_BTREE_LEAF_TABLE];
  u32 orphanBtreeLeafIndex = counts[PAGE_ORPHAN_BTREE_LEAF_INDEX];
  u32 orphanBtreeInteriorTable = counts[PAGE_ORPHAN_BTREE_INTERIOR_TABLE];
  u32 orphanBtreeInteriorIndex = counts[PAGE_ORPHAN_BTREE_INTERIOR_INDEX];
  u32 orphanOverflow = counts[PAGE_ORPHAN_OVERFLOW];
  u32 orphanEmpty = counts[PAGE_ORPHAN_EMPTY];
  u32 totalOrphan = orphanBtreeLeafTable + orphanBtreeLeafIndex +
                    orphanBtreeInteriorTable + orphanBtreeInteriorIndex +
                    orphanOverflow + orphanEmpty;

  if(totalOrphan > 0){
    printf("Orphaned (unaccounted) pages:\n");
    printf("  Orphan Btree Leaf Table:      %5u\n", orphanBtreeLeafTable);
    printf("  Orphan Btree Leaf Index:      %5u\n", orphanBtreeLeafIndex);
    printf("  Orphan Btree Interior Table:  %5u\n", orphanBtreeInteriorTable);
    printf("  Orphan Btree Interior Index:  %5u\n", orphanBtreeInteriorIndex);
    printf("  Orphan Overflow:              %5u\n", orphanOverflow);
    printf("  Orphan Empty:                 %5u\n", orphanEmpty);
    printf("  ────────────────────────────────────\n");
    printf("  Total orphaned:               %5u\n", totalOrphan);
    printf("\n");
  }

  printf("  UNKNOWN/Unclassified:  %5u\n", counts[PAGE_UNKNOWN]);
  printf("  ────────────────────────────\n");
  printf("  Total:                 %5u\n", g.mxPage);

  u32 totalFreelist = counts[PAGE_FREELIST_TRUNK] + counts[PAGE_FREELIST_LEAF];
  u32 totalBtree = counts[PAGE_BTREE_INTERIOR_INDEX] + counts[PAGE_BTREE_INTERIOR_TABLE] +
                   counts[PAGE_BTREE_LEAF_INDEX] + counts[PAGE_BTREE_LEAF_TABLE];
  u32 totalAccounted = totalFreelist + totalBtree + counts[PAGE_OVERFLOW] +
                       counts[PAGE_PTRMAP] + counts[PAGE_LOCKBYTE] + totalOrphan;

  printf("\n");
  printf("Summary:\n");
  printf("  Total freelist pages:  %u (header says %u)\n", totalFreelist, g.freelistCount);
  printf("  Total btree pages:     %u\n", totalBtree);
  printf("  Total overflow pages:  %u\n", counts[PAGE_OVERFLOW]);

  if(totalOrphan > 0){
    printf("  Total orphaned pages:  %u (%.2f MB wasted)\n",
           totalOrphan, totalOrphan * g.pagesize / (1024.0 * 1024.0));
    u32 orphanBtree = orphanBtreeLeafTable + orphanBtreeLeafIndex +
                      orphanBtreeInteriorTable + orphanBtreeInteriorIndex;
    printf("    - Orphan btree:      %u (%.2f MB)\n",
           orphanBtree, orphanBtree * g.pagesize / (1024.0 * 1024.0));
    printf("    - Orphan overflow:   %u (%.2f MB)\n",
           orphanOverflow, orphanOverflow * g.pagesize / (1024.0 * 1024.0));
    if(orphanEmpty > 0){
      printf("    - Orphan empty:      %u (%.2f MB)\n",
             orphanEmpty, orphanEmpty * g.pagesize / (1024.0 * 1024.0));
    }
  }

  printf("  Total accounted for:   %u\n", totalAccounted);
  printf("  Total unclassified:    %u\n", counts[PAGE_UNKNOWN]);

  if(totalFreelist != g.freelistCount){
    printf("\n⚠️  WARNING: Freelist count mismatch!\n");
    printf("   Found %u freelist pages but header says %u\n",
           totalFreelist, g.freelistCount);
    printf("   Difference: %d pages\n", (int)totalFreelist - (int)g.freelistCount);
  }

  /* Pointer map warnings */
  if(g.autoVacuum == 0 && g.ptrmapGhostCount > 0){
    printf("\n⚠️  WARNING: Ghost pointer map pages detected!\n");
    printf("   Auto-vacuum is DISABLED but %u pages at ptrmap positions\n", g.ptrmapGhostCount);
    printf("   contain valid ptrmap data. These are remnants from when\n");
    printf("   autovacuum was previously enabled.\n");
  }

  if(g.autoVacuum != 0 && counts[PAGE_PTRMAP] == 0){
    printf("\n⚠️  WARNING: Auto-vacuum enabled but NO ptrmap pages found!\n");
    printf("   This indicates database corruption.\n");
  }

  if(g.autoVacuum != 0 && g.ptrmapMissingCount > 0){
    printf("\n⚠️  WARNING: Missing or invalid pointer map pages!\n");
    printf("   Auto-vacuum is ENABLED but %u pages at ptrmap positions\n", g.ptrmapMissingCount);
    printf("   are missing or contain invalid data.\n");
 }

 /* Orphaned pages warning */
 if(totalOrphan > 0){
   printf("\n⚠️  WARNING: %u ORPHANED page(s) found! (%.2f MB wasted)\n",
          totalOrphan, totalOrphan * g.pagesize / (1024.0 * 1024.0));
   printf("   These pages contain data but are not referenced by any btree or freelist.\n");
   printf("   Run VACUUM to reclaim this space.\n");

   /* Write all orphaned pages to a file */
   FILE *fp = fopen("orphaned_pages.txt", "w");
   if(fp){
     for(i=0; i<numOrphaned; i++){
       fprintf(fp, "%u\n", orphanedPages[i]);
     }
     fclose(fp);
     printf("   All %u orphaned pages written to: orphaned_pages.txt\n", numOrphaned);
   }
 }

 if(counts[PAGE_UNKNOWN] > 0){
   printf("\n⚠️  WARNING: %u UNCLASSIFIED page(s) found!\n", counts[PAGE_UNKNOWN]);
    printf("   These pages are not in freelist, not in btrees, and not overflow.\n");

    /* Write all unaccounted pages to a file */
    FILE *fp = fopen("unaccounted_pages.txt", "w");
    if(fp){
      for(i=0; i<numUnknown; i++){
        fprintf(fp, "%u\n", unknownPages[i]);
      }
      fclose(fp);
      printf("   All %u unaccounted pages written to: unaccounted_pages.txt\n", numUnknown);
    }

    printf("   First %u unaccounted pages:\n", numUnknown < 20 ? numUnknown : 20);
    for(i=0; i<numUnknown && i<20; i++){
      printf("     Page %u\n", unknownPages[i]);
    }
    if(numUnknown > 20){
      printf("     ... and %u more\n", numUnknown - 20);
    }
  }else{
    printf("\n✓ All pages accounted for!\n");
  }

  free(unknownPages);
  free(orphanedPages);
}

/* Main */
int main(int argc, char **argv){
  if(argc != 2){
    fprintf(stderr, "Usage: %s DATABASE_FILE\n", argv[0]);
    return 1;
  }

  /* Open database file */
  g.fd = open(argv[1], O_RDONLY);
  if(g.fd < 0){
    fprintf(stderr, "ERROR: cannot open '%s'\n", argv[1]);
    return 1;
  }

  /* Read header */
  if(readHeader()){
    close(g.fd);
    return 1;
  }

  printf("Database: %s\n", argv[1]);
  printf("Page size: %u bytes\n", g.pagesize);
  printf("Total pages: %u\n", g.mxPage);
  printf("\n");

  /* Allocate tracking arrays */
  g.pageTypes = calloc(g.mxPage + 1, sizeof(PageType));
  g.pageParents = calloc(g.mxPage + 1, sizeof(u32));
  if(!g.pageTypes || !g.pageParents){
    fprintf(stderr, "ERROR: out of memory\n");
    close(g.fd);
    return 1;
  }

  printf("Walking freelist...\n");
  if(walkFreelist()){
    close(g.fd);
    return 1;
  }

  printf("Marking pointer map pages...\n");
  markPtrmapPages();

  printf("Walking all btrees...\n");
  if(walkAllBtrees(argv[1])){
    close(g.fd);
    return 1;
  }

  printf("Classifying orphaned pages...\n");
  classifyOrphanedPages();

  /* Print report */
  printReport();

  /* Cleanup */
  free(g.pageTypes);
  free(g.pageParents);
  close(g.fd);

  return 0;
}
