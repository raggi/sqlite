/*
** This file implements a utility program that extracts raw record data
** from a SQLite database table by rowid.
**
** Usage:
**
**    dumprow DATABASE TABLE ROWID
**
** This tool walks the btree for the specified table and extracts the
** raw record bytes for the given rowid, even if the record is corrupt.
**
** Build:
**    gcc -o dumprow dumprow.c
*/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

/* SQLite database file format constants */
#define SQLITE_HEADER_SIZE 100
#define OFFSET_PAGE_SIZE 16
#define OFFSET_RESERVED_SPACE 20

/* B-tree page type constants */
#define BTREE_INTERIOR_INDEX 0x02
#define BTREE_INTERIOR_TABLE 0x05
#define BTREE_LEAF_INDEX 0x0a
#define BTREE_LEAF_TABLE 0x0d

/* B-tree header offsets */
#define BTREE_HEADER_OFFSET_TYPE 0
#define BTREE_HEADER_OFFSET_CELL_COUNT 3
#define BTREE_HEADER_OFFSET_RIGHTMOST 8

typedef struct {
  uint32_t pageSize;
  uint32_t reservedSpace;
  uint32_t totalPages;
  FILE *db;
  unsigned char *pageBuf;
  uint64_t targetRowid;
  int found;
} DbContext;

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

/* Read a varint, return bytes consumed */
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

/* Read a page */
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

/* Dump record as hex and attempt to parse */
static void dumpRecord(const unsigned char *record, uint32_t size) {
  uint32_t i;
  uint64_t headerSize;
  int n, pos;

  printf("\n=== RAW RECORD DATA ===\n");
  printf("Record size: %u bytes\n\n", size);

  /* Hex dump */
  printf("Hex dump:\n");
  for(i=0; i<size; i++){
    if( i>0 && i%16==0 ) printf("\n");
    printf("%02x ", record[i]);
  }
  printf("\n\n");

  /* Try to parse record header */
  n = readVarint(record, &headerSize);
  if( headerSize > size || headerSize > 10000 ){
    printf("ERROR: Invalid header size %llu\n", (unsigned long long)headerSize);
    return;
  }

  printf("Record header size: %llu bytes\n", (unsigned long long)headerSize);
  printf("Record header (hex): ");
  for(i=0; i<headerSize && i<size; i++){
    printf("%02x ", record[i]);
  }
  printf("\n\n");

  /* Parse serial types */
  printf("Column serial types:\n");
  pos = n;
  int colNum = 0;
  while( pos < headerSize ){
    uint64_t serialType;
    int m = readVarint(&record[pos], &serialType);
    pos += m;
    printf("  Column %d: serial type %llu", colNum, (unsigned long long)serialType);

    /* Interpret serial type */
    if( serialType == 0 ){
      printf(" (NULL)");
    } else if( serialType >= 1 && serialType <= 6 ){
      printf(" (integer, %llu bytes)", (unsigned long long)serialType);
    } else if( serialType == 7 ){
      printf(" (float, 8 bytes)");
    } else if( serialType == 8 ){
      printf(" (integer 0)");
    } else if( serialType == 9 ){
      printf(" (integer 1)");
    } else if( serialType >= 12 && serialType%2 == 0 ){
      uint64_t len = (serialType - 12) / 2;
      printf(" (BLOB, %llu bytes)", (unsigned long long)len);
    } else if( serialType >= 13 && serialType%2 == 1 ){
      uint64_t len = (serialType - 13) / 2;
      printf(" (TEXT, %llu bytes)", (unsigned long long)len);
    }
    printf("\n");
    colNum++;
  }
  printf("\n");

  /* Dump column data */
  printf("Column data:\n");
  pos = headerSize;
  colNum = 0;
  int colPos = n;
  while( colPos < headerSize && pos < size ){
    uint64_t serialType;
    int m = readVarint(&record[colPos], &serialType);
    colPos += m;

    printf("  Column %d: ", colNum);

    if( serialType == 0 ){
      printf("NULL\n");
    } else if( serialType == 1 ){
      if( pos < size ){
        printf("%d\n", (int8_t)record[pos]);
        pos += 1;
      }
    } else if( serialType == 2 ){
      if( pos+1 < size ){
        printf("%d\n", (int16_t)read16(&record[pos]));
        pos += 2;
      }
    } else if( serialType == 3 ){
      if( pos+2 < size ){
        uint32_t val = (record[pos]<<16) | read16(&record[pos+1]);
        printf("%u\n", val);
        pos += 3;
      }
    } else if( serialType == 4 ){
      if( pos+3 < size ){
        printf("%u\n", read32(&record[pos]));
        pos += 4;
      }
    } else if( serialType == 8 ){
      printf("0\n");
    } else if( serialType == 9 ){
      printf("1\n");
    } else if( serialType >= 13 && serialType%2 == 1 ){
      uint64_t len = (serialType - 13) / 2;
      printf("\"");
      uint32_t printLen = len < 200 ? len : 200;
      for(i=0; i<printLen && pos+i<size; i++){
        unsigned char c = record[pos+i];
        if( c >= 32 && c < 127 ){
          printf("%c", c);
        } else {
          printf(".");
        }
      }
      if( len > 200 ) printf("... (truncated, total %llu bytes)", (unsigned long long)len);
      printf("\"\n");
      pos += len;
    } else {
      /* BLOB or other type */
      uint64_t len = 0;
      if( serialType >= 12 && serialType%2 == 0 ){
        len = (serialType - 12) / 2;
      } else if( serialType >= 1 && serialType <= 7 ){
        len = serialType;
        if( serialType == 7 ) len = 8;
      }
      printf("(binary, %llu bytes): ", (unsigned long long)len);
      uint32_t printLen = len < 32 ? len : 32;
      for(i=0; i<printLen && pos+i<size; i++){
        printf("%02x ", record[pos+i]);
      }
      if( len > 32 ) printf("...");
      printf("\n");
      pos += len;
    }

    colNum++;
  }
}

/* Search for target rowid in a leaf page */
static void searchLeaf(DbContext *ctx, unsigned char *page, int headerOffset) {
  uint32_t cellCount = read16(&page[headerOffset + 3]);
  uint32_t i;

  for(i=0; i<cellCount; i++){
    uint32_t cellOffset = read16(&page[headerOffset + 8 + 12 + i*2]);
    if( cellOffset < headerOffset + 8 ) continue;
    if( cellOffset >= ctx->pageSize - ctx->reservedSpace ) continue;

    /* Parse cell */
    uint64_t payloadSize, rowid;
    int n = readVarint(&page[cellOffset], &payloadSize);
    n += readVarint(&page[cellOffset + n], &rowid);

    if( rowid == ctx->targetRowid ){
      printf("Found target rowid %llu!\n", (unsigned long long)rowid);
      printf("Cell offset in page: %u\n", cellOffset);
      printf("Payload size: %llu bytes\n", (unsigned long long)payloadSize);

      /* Calculate local payload size */
      uint32_t usableSize = ctx->pageSize - ctx->reservedSpace;
      uint32_t maxLocal = usableSize - 35;
      uint32_t minLocal = ((usableSize - 12) * 32 / 255) - 23;
      uint32_t local;

      if( payloadSize <= maxLocal ){
        local = payloadSize;
      } else {
        local = minLocal + ((payloadSize - minLocal) % (usableSize - 4));
      }

      printf("Local payload: %u bytes\n", local);

      /* Check for overflow */
      if( payloadSize > local ){
        uint32_t overflowPgno = read32(&page[cellOffset + n + local]);
        printf("Has overflow pages starting at page %u\n", overflowPgno);
        printf("WARNING: This tool does not yet handle overflow pages.\n");
        printf("Dumping local payload only:\n");
      }

      /* Dump the record */
      dumpRecord(&page[cellOffset + n], local);
      ctx->found = 1;
      return;
    }
  }
}

/* Walk btree to find rowid */
static void walkBtree(DbContext *ctx, uint32_t pgno) {
  unsigned char *page = ctx->pageBuf;
  uint8_t pageType;
  int headerOffset;

  if( ctx->found ) return;
  if( pgno==0 || pgno>ctx->totalPages ) return;

  if( readPage(ctx, pgno, page) != 0 ){
    fprintf(stderr, "Failed to read page %u\n", pgno);
    return;
  }

  headerOffset = (pgno==1) ? 100 : 0;
  pageType = page[headerOffset];

  if( pageType == BTREE_LEAF_TABLE ){
    searchLeaf(ctx, page, headerOffset);
  } else if( pageType == BTREE_INTERIOR_TABLE ){
    uint32_t cellCount = read16(&page[headerOffset + 3]);
    uint32_t i;

    /* Walk interior cells */
    for(i=0; i<cellCount; i++){
      uint32_t cellOffset = read16(&page[headerOffset + 8 + i*2]);
      if( cellOffset < headerOffset + 8 ) continue;

      uint32_t childPgno = read32(&page[cellOffset]);
      uint64_t key;
      readVarint(&page[cellOffset + 4], &key);

      if( ctx->targetRowid <= key ){
        walkBtree(ctx, childPgno);
        if( ctx->found ) return;
      }
    }

    /* Walk rightmost child */
    uint32_t rightmost = read32(&page[headerOffset + 8]);
    walkBtree(ctx, rightmost);
  }
}

int main(int argc, char **argv) {
  DbContext ctx;
  unsigned char header[SQLITE_HEADER_SIZE];
  unsigned char schemaPage[65536];
  uint32_t rootPage = 0;

  if( argc != 4 ){
    fprintf(stderr, "Usage: %s DATABASE TABLE ROWID\n", argv[0]);
    fprintf(stderr, "\n");
    fprintf(stderr, "Extract raw record data for a specific rowid.\n");
    fprintf(stderr, "Example: %s mydb.db MyTable 12345\n", argv[0]);
    return 1;
  }

  memset(&ctx, 0, sizeof(ctx));
  ctx.targetRowid = strtoull(argv[3], NULL, 10);

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

  /* Verify magic */
  if( memcmp(header, "SQLite format 3\000", 16) != 0 ){
    fprintf(stderr, "%s is not a valid SQLite database\n", argv[1]);
    fclose(ctx.db);
    return 1;
  }

  /* Parse header */
  ctx.pageSize = readPageSize(header);
  ctx.reservedSpace = header[OFFSET_RESERVED_SPACE];
  ctx.totalPages = read32(&header[28]);

  printf("Database: %s\n", argv[1]);
  printf("Table: %s\n", argv[2]);
  printf("Target rowid: %llu\n", (unsigned long long)ctx.targetRowid);
  printf("Page size: %u bytes\n", ctx.pageSize);
  printf("Total pages: %u\n\n", ctx.totalPages);

  /* Allocate page buffer */
  ctx.pageBuf = malloc(ctx.pageSize);
  if( !ctx.pageBuf ){
    fprintf(stderr, "Out of memory\n");
    fclose(ctx.db);
    return 1;
  }

  /* Read schema page to find table root */
  if( readPage(&ctx, 1, schemaPage) != 0 ){
    fprintf(stderr, "Cannot read schema page\n");
    free(ctx.pageBuf);
    fclose(ctx.db);
    return 1;
  }

  /* Parse schema (simplified - assumes schema fits in page 1) */
  int headerOffset = 100;
  uint8_t pageType = schemaPage[headerOffset];
  if( pageType != BTREE_LEAF_TABLE ){
    fprintf(stderr, "Schema table has multiple pages - not supported\n");
    free(ctx.pageBuf);
    fclose(ctx.db);
    return 1;
  }

  uint32_t cellCount = read16(&schemaPage[headerOffset + 3]);
  uint32_t i;
  for(i=0; i<cellCount; i++){
    uint32_t cellOffset = read16(&schemaPage[headerOffset + 8 + 12 + i*2]);
    if( cellOffset < headerOffset + 8 ) continue;

    uint64_t payloadSize, rowid;
    int n = readVarint(&schemaPage[cellOffset], &payloadSize);
    n += readVarint(&schemaPage[cellOffset + n], &rowid);

    unsigned char *record = &schemaPage[cellOffset + n];
    uint64_t hdrSize;
    int m = readVarint(record, &hdrSize);

    /* Parse serial types to get field sizes */
    uint64_t serialTypes[5];
    int pos = m;
    int j;
    for(j=0; j<5 && pos<hdrSize; j++){
      pos += readVarint(&record[pos], &serialTypes[j]);
    }

    /* Extract name field (column 1) */
    int dataPos = hdrSize;

    /* Skip type (column 0) */
    if( serialTypes[0]>=13 && serialTypes[0]%2==1 ){
      dataPos += (serialTypes[0] - 13) / 2;
    }

    /* Get name */
    int nameLen = 0;
    if( serialTypes[1]>=13 && serialTypes[1]%2==1 ){
      nameLen = (serialTypes[1] - 13) / 2;
    }

    char name[256] = {0};
    if( nameLen > 0 && nameLen < 256 ){
      memcpy(name, &record[dataPos], nameLen);
    }
    dataPos += nameLen;

    /* Skip tbl_name (column 2) */
    if( serialTypes[2]>=13 && serialTypes[2]%2==1 ){
      dataPos += (serialTypes[2] - 13) / 2;
    }

    /* Get rootpage (column 3) */
    uint32_t rp = 0;
    if( serialTypes[3]==1 ){
      rp = record[dataPos];
    } else if( serialTypes[3]==2 ){
      rp = read16(&record[dataPos]);
    } else if( serialTypes[3]==3 ){
      rp = (record[dataPos]<<16) | read16(&record[dataPos+1]);
    } else if( serialTypes[3]==4 ){
      rp = read32(&record[dataPos]);
    }

    if( strcmp(name, argv[2]) == 0 && rp > 0 ){
      rootPage = rp;
      break;
    }
  }

  if( rootPage == 0 ){
    fprintf(stderr, "Table '%s' not found in database\n", argv[2]);
    free(ctx.pageBuf);
    fclose(ctx.db);
    return 1;
  }

  printf("Table root page: %u\n\n", rootPage);
  printf("Searching for rowid %llu...\n\n", (unsigned long long)ctx.targetRowid);

  /* Walk the btree */
  walkBtree(&ctx, rootPage);

  if( !ctx.found ){
    printf("Rowid %llu not found in table '%s'\n",
           (unsigned long long)ctx.targetRowid, argv[2]);
  }

  free(ctx.pageBuf);
  fclose(ctx.db);
  return ctx.found ? 0 : 1;
}
