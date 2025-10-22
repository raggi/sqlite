/*
** This file implements a utility program that identifies which table
** or index owns specific pages in a SQLite database.
**
** Usage:
**
**    pageowner DATABASE PAGE [PAGE ...]
**
** For each page number provided, this tool walks all btrees in the
** database and reports which table/index contains that page.
**
** Build:
**    gcc -o pageowner pageowner.c
*/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

/* SQLite database file format constants */
#define SQLITE_HEADER_SIZE 100
#define OFFSET_PAGE_SIZE 16
#define OFFSET_RESERVED_SPACE 20

/* B-tree page type constants (first byte of page) */
#define BTREE_INTERIOR_INDEX 0x02
#define BTREE_INTERIOR_TABLE 0x05
#define BTREE_LEAF_INDEX 0x0a
#define BTREE_LEAF_TABLE 0x0d

/* B-tree page header offsets */
#define BTREE_HEADER_OFFSET_TYPE 0
#define BTREE_HEADER_OFFSET_FIRST_FREEBLOCK 1
#define BTREE_HEADER_OFFSET_CELL_COUNT 3
#define BTREE_HEADER_OFFSET_CELL_CONTENT 5
#define BTREE_HEADER_OFFSET_FRAGMENTED 7
#define BTREE_HEADER_OFFSET_RIGHTMOST 8  /* Interior pages only */

/* Schema table constants */
#define SCHEMA_ROOT_PAGE 1

/* Maximum database size we can handle */
#define MAX_PAGES 100000000

typedef struct {
  uint32_t pageSize;
  uint32_t reservedSpace;
  uint32_t totalPages;
  unsigned char *pageOwner;  /* pageOwner[pgno] = owner bitmap */
  FILE *db;
  unsigned char *pageBuf;
} DbContext;

typedef struct {
  char type[16];      /* "table" or "index" */
  char name[256];
  uint32_t rootpage;
} SchemaEntry;

/* Read a 32-bit big-endian integer */
static uint32_t read32(const unsigned char *p) {
  return (p[0]<<24) | (p[1]<<16) | (p[2]<<8) | p[3];
}

/* Read a 16-bit big-endian integer */
static uint32_t read16(const unsigned char *p) {
  return (p[0]<<8) | p[1];
}

/* Read database page size from header */
static uint32_t readPageSize(const unsigned char *header) {
  uint32_t sz = read16(&header[OFFSET_PAGE_SIZE]);
  if( sz==1 ) sz = 65536;
  return sz;
}

/* Read a varint from buffer, return bytes consumed */
static int readVarint(const unsigned char *p, uint64_t *result) {
  uint64_t v = 0;
  int i;
  for(i=0; i<8; i++){
    v = (v<<7) | (p[i] & 0x7f);
    if( (p[i] & 0x80)==0 ){
      *result = v;
      return i+1;
    }
  }
  v = (v<<8) | p[8];
  *result = v;
  return 9;
}

/* Read a page from the database */
static int readPage(DbContext *ctx, uint32_t pgno, unsigned char *buf) {
  if( pgno==0 || pgno>ctx->totalPages ){
    return -1;
  }
  if( fseek(ctx->db, (long long)(pgno-1) * ctx->pageSize, SEEK_SET) != 0 ){
    return -1;
  }
  if( fread(buf, 1, ctx->pageSize, ctx->db) != ctx->pageSize ){
    return -1;
  }
  return 0;
}

/* Mark a page as owned */
static void markPageOwned(DbContext *ctx, uint32_t pgno) {
  if( pgno>0 && pgno<=ctx->totalPages ){
    ctx->pageOwner[pgno] = 1;
  }
}

/* Check if a page is owned */
static int isPageOwned(DbContext *ctx, uint32_t pgno) {
  if( pgno>0 && pgno<=ctx->totalPages ){
    return ctx->pageOwner[pgno];
  }
  return 0;
}

/* Walk a btree starting at rootPage and mark all pages as owned */
static void walkBtree(DbContext *ctx, uint32_t pgno) {
  unsigned char *page = ctx->pageBuf;
  uint32_t cellCount, i, offset;
  uint8_t pageType;
  int headerOffset;

  if( pgno==0 || pgno>ctx->totalPages ) return;
  if( isPageOwned(ctx, pgno) ) return;  /* Already visited */

  if( readPage(ctx, pgno, page) != 0 ) return;

  /* Mark this page as owned */
  markPageOwned(ctx, pgno);

  /* Page 1 has a 100-byte database header before the btree header */
  headerOffset = (pgno==1) ? 100 : 0;

  pageType = page[headerOffset + BTREE_HEADER_OFFSET_TYPE];
  cellCount = read16(&page[headerOffset + BTREE_HEADER_OFFSET_CELL_COUNT]);

  /* Handle overflow pages for leaf pages */
  if( pageType==BTREE_LEAF_TABLE ){
    /* Walk cells to find overflow pages */
    for(i=0; i<cellCount; i++){
      uint32_t cellOffset = read16(&page[headerOffset + 8 + 12 + i*2]);
      if( cellOffset < headerOffset + 8 ) continue;
      if( cellOffset >= ctx->pageSize - ctx->reservedSpace ) continue;

      uint64_t payloadSize, rowid;
      int n = readVarint(&page[cellOffset], &payloadSize);
      n += readVarint(&page[cellOffset + n], &rowid);

      /* Calculate if there are overflow pages */
      uint32_t usableSize = ctx->pageSize - ctx->reservedSpace;
      uint32_t maxLocal = usableSize - 35;
      uint32_t minLocal = ((usableSize - 12) * 32 / 255) - 23;
      uint32_t local;

      if( payloadSize <= maxLocal ){
        local = payloadSize;
      } else {
        local = minLocal + ((payloadSize - minLocal) % (usableSize - 4));
      }

      if( payloadSize > local ){
        /* There are overflow pages */
        uint32_t overflowPgno = read32(&page[cellOffset + n + local]);
        while( overflowPgno != 0 ){
          markPageOwned(ctx, overflowPgno);
          unsigned char overflowPage[4096];
          if( readPage(ctx, overflowPgno, overflowPage) != 0 ) break;
          overflowPgno = read32(overflowPage);
        }
      }
    }
  } else if( pageType==BTREE_LEAF_INDEX ){
    /* Walk cells to find overflow pages */
    for(i=0; i<cellCount; i++){
      uint32_t cellOffset = read16(&page[headerOffset + 8 + i*2]);
      if( cellOffset < headerOffset + 8 ) continue;
      if( cellOffset >= ctx->pageSize - ctx->reservedSpace ) continue;

      uint64_t payloadSize;
      int n = readVarint(&page[cellOffset], &payloadSize);

      /* Calculate if there are overflow pages */
      uint32_t usableSize = ctx->pageSize - ctx->reservedSpace;
      uint32_t maxLocal = ((usableSize - 12) * 64 / 255) - 23;
      uint32_t minLocal = ((usableSize - 12) * 32 / 255) - 23;
      uint32_t local;

      if( payloadSize <= maxLocal ){
        local = payloadSize;
      } else {
        local = minLocal + ((payloadSize - minLocal) % (usableSize - 4));
      }

      if( payloadSize > local ){
        /* There are overflow pages */
        uint32_t overflowPgno = read32(&page[cellOffset + n + local]);
        while( overflowPgno != 0 ){
          markPageOwned(ctx, overflowPgno);
          unsigned char overflowPage[4096];
          if( readPage(ctx, overflowPgno, overflowPage) != 0 ) break;
          overflowPgno = read32(overflowPage);
        }
      }
    }
  }

  /* For interior pages, recursively walk child pages */
  if( pageType==BTREE_INTERIOR_TABLE || pageType==BTREE_INTERIOR_INDEX ){
    /* Walk all child pointers in cells */
    for(i=0; i<cellCount; i++){
      offset = read16(&page[headerOffset + 8 + i*2]);
      if( offset < headerOffset + 8 ) continue;
      uint32_t childPgno = read32(&page[offset]);
      walkBtree(ctx, childPgno);
    }
    /* Walk rightmost child pointer */
    uint32_t rightmost = read32(&page[headerOffset + BTREE_HEADER_OFFSET_RIGHTMOST]);
    walkBtree(ctx, rightmost);
  }
}

/* Read schema entries from the database */
static int readSchema(DbContext *ctx, SchemaEntry **entries, int *count) {
  unsigned char *page = ctx->pageBuf;
  int i;
  SchemaEntry *list = NULL;
  int capacity = 100;
  int nEntries = 0;

  list = malloc(capacity * sizeof(SchemaEntry));
  if( !list ) return -1;

  /* Read page 1 (schema table root) */
  if( readPage(ctx, SCHEMA_ROOT_PAGE, page) != 0 ){
    free(list);
    return -1;
  }

  /* Page 1 has 100-byte database header */
  int headerOffset = 100;
  uint8_t pageType = page[headerOffset + BTREE_HEADER_OFFSET_TYPE];
  uint32_t cellCount = read16(&page[headerOffset + BTREE_HEADER_OFFSET_CELL_COUNT]);

  if( pageType != BTREE_LEAF_TABLE ){
    /* Schema table has grown beyond one page - not handling this case */
    free(list);
    return -1;
  }

  /* Walk schema table cells */
  for(i=0; i<cellCount; i++){
    uint32_t cellOffset = read16(&page[headerOffset + 8 + 12 + i*2]);
    if( cellOffset < headerOffset + 8 ) continue;

    uint64_t payloadSize, rowid;
    int n = readVarint(&page[cellOffset], &payloadSize);
    n += readVarint(&page[cellOffset + n], &rowid);

    /* Parse record header and extract fields */
    unsigned char *record = &page[cellOffset + n];
    uint64_t headerSize;
    int m = readVarint(record, &headerSize);

    /* Read serial types for each column */
    uint64_t serialTypes[5];
    int pos = m;
    for(int j=0; j<5 && pos<headerSize; j++){
      pos += readVarint(&record[pos], &serialTypes[j]);
    }

    /* Extract type (column 0), name (column 1), and rootpage (column 3) */
    int dataPos = headerSize;

    /* Column 0: type */
    int typeLen = 0;
    if( serialTypes[0]>=13 && (serialTypes[0]%2)==1 ){
      typeLen = (serialTypes[0] - 13) / 2;
    }
    if( typeLen > 0 && typeLen < sizeof(list[nEntries].type) ){
      memcpy(list[nEntries].type, &record[dataPos], typeLen);
      list[nEntries].type[typeLen] = '\0';
    }
    dataPos += typeLen;

    /* Column 1: name */
    int nameLen = 0;
    if( serialTypes[1]>=13 && (serialTypes[1]%2)==1 ){
      nameLen = (serialTypes[1] - 13) / 2;
    }
    if( nameLen > 0 && nameLen < sizeof(list[nEntries].name) ){
      memcpy(list[nEntries].name, &record[dataPos], nameLen);
      list[nEntries].name[nameLen] = '\0';
    }
    dataPos += nameLen;

    /* Column 2: tbl_name (skip) */
    int tblNameLen = 0;
    if( serialTypes[2]>=13 && (serialTypes[2]%2)==1 ){
      tblNameLen = (serialTypes[2] - 13) / 2;
    }
    dataPos += tblNameLen;

    /* Column 3: rootpage */
    if( serialTypes[3]==1 ){
      list[nEntries].rootpage = record[dataPos];
    } else if( serialTypes[3]==2 ){
      list[nEntries].rootpage = read16(&record[dataPos]);
    } else if( serialTypes[3]==3 ){
      list[nEntries].rootpage = (record[dataPos]<<16) | read16(&record[dataPos+1]);
    } else if( serialTypes[3]==4 ){
      list[nEntries].rootpage = read32(&record[dataPos]);
    }

    if( list[nEntries].rootpage > 0 ){
      nEntries++;
      if( nEntries >= capacity ){
        capacity *= 2;
        SchemaEntry *newList = realloc(list, capacity * sizeof(SchemaEntry));
        if( !newList ){
          free(list);
          return -1;
        }
        list = newList;
      }
    }
  }

  *entries = list;
  *count = nEntries;
  return 0;
}

int main(int argc, char **argv) {
  DbContext ctx;
  unsigned char header[SQLITE_HEADER_SIZE];
  SchemaEntry *schema;
  int schemaCount;
  int i, j;

  if( argc < 3 ){
    fprintf(stderr, "Usage: %s DATABASE PAGE [PAGE ...]\n", argv[0]);
    fprintf(stderr, "\n");
    fprintf(stderr, "Identify which table/index owns the specified pages.\n");
    return 1;
  }

  memset(&ctx, 0, sizeof(ctx));

  /* Open database */
  ctx.db = fopen(argv[1], "rb");
  if( !ctx.db ){
    fprintf(stderr, "Cannot open %s\n", argv[1]);
    return 1;
  }

  /* Read header */
  if( fread(header, 1, SQLITE_HEADER_SIZE, ctx.db) != SQLITE_HEADER_SIZE ){
    fprintf(stderr, "Cannot read database header\n");
    fclose(ctx.db);
    return 1;
  }

  /* Verify magic string */
  if( memcmp(header, "SQLite format 3\000", 16) != 0 ){
    fprintf(stderr, "%s is not a valid SQLite database\n", argv[1]);
    fclose(ctx.db);
    return 1;
  }

  /* Parse header */
  ctx.pageSize = readPageSize(header);
  ctx.reservedSpace = header[OFFSET_RESERVED_SPACE];
  ctx.totalPages = read32(&header[28]);

  if( ctx.totalPages > MAX_PAGES ){
    fprintf(stderr, "Database too large (%u pages)\n", ctx.totalPages);
    fclose(ctx.db);
    return 1;
  }

  /* Allocate buffers */
  ctx.pageOwner = calloc(ctx.totalPages + 1, 1);
  ctx.pageBuf = malloc(ctx.pageSize);
  if( !ctx.pageOwner || !ctx.pageBuf ){
    fprintf(stderr, "Out of memory\n");
    free(ctx.pageOwner);
    free(ctx.pageBuf);
    fclose(ctx.db);
    return 1;
  }

  /* Read schema */
  if( readSchema(&ctx, &schema, &schemaCount) != 0 ){
    fprintf(stderr, "Failed to read schema\n");
    free(ctx.pageOwner);
    free(ctx.pageBuf);
    fclose(ctx.db);
    return 1;
  }

  printf("Database: %s\n", argv[1]);
  printf("Page size: %u bytes\n", ctx.pageSize);
  printf("Total pages: %u\n", ctx.totalPages);
  printf("Schema entries: %d\n\n", schemaCount);

  /* For each requested page, walk all btrees and find the owner */
  for(i=2; i<argc; i++){
    uint32_t targetPage = atoi(argv[i]);
    int found = 0;

    printf("Page %u:\n", targetPage);

    if( targetPage == 0 || targetPage > ctx.totalPages ){
      printf("  ERROR: Invalid page number\n\n");
      continue;
    }

    /* Check each schema entry */
    for(j=0; j<schemaCount; j++){
      /* Clear ownership bitmap */
      memset(ctx.pageOwner, 0, ctx.totalPages + 1);

      /* Walk this btree */
      walkBtree(&ctx, schema[j].rootpage);

      /* Check if target page is owned */
      if( isPageOwned(&ctx, targetPage) ){
        printf("  Owned by: %s '%s' (root page %u)\n",
               schema[j].type, schema[j].name, schema[j].rootpage);
        found = 1;
      }
    }

    if( !found ){
      printf("  Not found in any table/index (possibly freelist, lock-byte page, or ptrmap)\n");
    }
    printf("\n");
  }

  free(schema);
  free(ctx.pageOwner);
  free(ctx.pageBuf);
  fclose(ctx.db);
  return 0;
}
