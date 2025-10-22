/*
** This file implements a utility program that walks all pages of a
** SQLite table and validates/reports on every cell.
**
** Usage:
**
**    tablewalk DATABASE TABLE [OPTIONS]
**
** Options:
**    --find-rowid=ROWID   Search for and dump a specific rowid
**    --verbose            Print info about every page
**    --validate           Perform validation checks on all cells
**
** This tool performs a raw page-by-page scan of a table's btree,
** reading every cell even if it's corrupt or malformed. It reports
** on structure, validates cell data, and can extract specific rowids
** that normal SQLite queries cannot read.
**
** Build:
**    gcc -o tablewalk tablewalk.c
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
#define BTREE_HEADER_OFFSET_FREEBLOCK 1
#define BTREE_HEADER_OFFSET_CELL_COUNT 3
#define BTREE_HEADER_OFFSET_CELL_CONTENT 5
#define BTREE_HEADER_OFFSET_FRAGMENTED 7
#define BTREE_HEADER_OFFSET_RIGHTMOST 8

typedef struct {
  FILE *db;
  uint32_t pageSize;
  uint32_t reservedSpace;
  uint32_t totalPages;
  unsigned char *pageBuf;

  /* Options */
  uint64_t findRowid;
  int verbose;
  int validate;
  int foundTarget;

  /* Statistics */
  uint32_t pagesScanned;
  uint32_t leafPagesScanned;
  uint32_t interiorPagesScanned;
  uint32_t cellsScanned;
  uint32_t corruptCells;
  uint64_t minRowid;
  uint64_t maxRowid;
} WalkContext;

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

/* Read a page from the database */
static int readPage(WalkContext *ctx, uint32_t pgno, unsigned char *buf) {
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

/* Dump a record in detail */
static void dumpRecord(const unsigned char *record, uint32_t size, uint64_t rowid) {
  uint32_t i;
  uint64_t headerSize;
  int n, pos, colNum, dataPos;

  printf("\n========================================\n");
  printf("RECORD FOUND: rowid = %llu\n", (unsigned long long)rowid);
  printf("========================================\n\n");

  printf("Record size: %u bytes\n\n", size);

  /* Full hex dump */
  printf("Complete hex dump:\n");
  for(i=0; i<size; i++){
    if( i>0 && i%32==0 ) printf("\n");
    printf("%02x", record[i]);
    if((i+1)%4==0 && (i+1)%32!=0) printf(" ");
  }
  printf("\n\n");

  /* Parse record header */
  n = readVarint(record, &headerSize);
  if( headerSize > size || headerSize > 10000 ){
    printf("ERROR: Invalid header size %llu (record size %u)\n",
           (unsigned long long)headerSize, size);
    printf("This record is corrupt - header size is unreasonable.\n");
    return;
  }

  printf("Record header size: %llu bytes\n", (unsigned long long)headerSize);
  printf("Header bytes: ");
  for(i=0; i<headerSize && i<size; i++){
    printf("%02x ", record[i]);
  }
  printf("\n\n");

  /* Parse serial types */
  printf("Column serial types:\n");
  pos = n;
  colNum = 0;
  while( pos < headerSize ){
    uint64_t serialType;
    int m = readVarint(&record[pos], &serialType);
    pos += m;
    printf("  Column %d: serial type %llu", colNum, (unsigned long long)serialType);

    /* Interpret serial type */
    if( serialType == 0 ){
      printf(" (NULL)\n");
    } else if( serialType == 1 ){
      printf(" (8-bit signed integer)\n");
    } else if( serialType == 2 ){
      printf(" (16-bit signed integer)\n");
    } else if( serialType == 3 ){
      printf(" (24-bit signed integer)\n");
    } else if( serialType == 4 ){
      printf(" (32-bit signed integer)\n");
    } else if( serialType == 5 ){
      printf(" (48-bit signed integer)\n");
    } else if( serialType == 6 ){
      printf(" (64-bit signed integer)\n");
    } else if( serialType == 7 ){
      printf(" (64-bit float)\n");
    } else if( serialType == 8 ){
      printf(" (integer constant 0)\n");
    } else if( serialType == 9 ){
      printf(" (integer constant 1)\n");
    } else if( serialType >= 12 && serialType%2 == 0 ){
      uint64_t len = (serialType - 12) / 2;
      printf(" (BLOB, %llu bytes)\n", (unsigned long long)len);
    } else if( serialType >= 13 && serialType%2 == 1 ){
      uint64_t len = (serialType - 13) / 2;
      printf(" (TEXT, %llu bytes)\n", (unsigned long long)len);
    } else {
      printf(" (UNKNOWN/INVALID)\n");
    }
    colNum++;
  }
  printf("\n");

  /* Parse and display column data */
  printf("Column values:\n");
  pos = n;
  colNum = 0;
  dataPos = headerSize;

  while( pos < headerSize && dataPos <= size ){
    uint64_t serialType;
    int m = readVarint(&record[pos], &serialType);
    pos += m;

    printf("  Column %d: ", colNum);

    if( serialType == 0 ){
      printf("NULL\n");
    } else if( serialType == 1 ){
      if( dataPos < size ){
        printf("%d\n", (int8_t)record[dataPos]);
        dataPos += 1;
      } else {
        printf("ERROR: Not enough data\n");
      }
    } else if( serialType == 2 ){
      if( dataPos+1 < size ){
        int16_t val = (int16_t)read16(&record[dataPos]);
        printf("%d\n", val);
        dataPos += 2;
      } else {
        printf("ERROR: Not enough data\n");
      }
    } else if( serialType == 3 ){
      if( dataPos+2 < size ){
        int32_t val = (record[dataPos]<<16) | read16(&record[dataPos+1]);
        if( val & 0x800000 ) val |= 0xFF000000; /* sign extend */
        printf("%d\n", val);
        dataPos += 3;
      } else {
        printf("ERROR: Not enough data\n");
      }
    } else if( serialType == 4 ){
      if( dataPos+3 < size ){
        int32_t val = (int32_t)read32(&record[dataPos]);
        printf("%d\n", val);
        dataPos += 4;
      } else {
        printf("ERROR: Not enough data\n");
      }
    } else if( serialType == 8 ){
      printf("0\n");
    } else if( serialType == 9 ){
      printf("1\n");
    } else if( serialType >= 13 && serialType%2 == 1 ){
      uint64_t len = (serialType - 13) / 2;
      if( dataPos + len <= size ){
        printf("\"");
        uint32_t printLen = len < 500 ? len : 500;
        for(i=0; i<printLen; i++){
          unsigned char c = record[dataPos+i];
          if( c >= 32 && c < 127 && c != '"' && c != '\\' ){
            printf("%c", c);
          } else if( c == '"' ){
            printf("\\\"");
          } else if( c == '\\' ){
            printf("\\\\");
          } else if( c == '\n' ){
            printf("\\n");
          } else if( c == '\r' ){
            printf("\\r");
          } else if( c == '\t' ){
            printf("\\t");
          } else {
            printf("\\x%02x", c);
          }
        }
        if( len > 500 ){
          printf("... (truncated, total %llu bytes)", (unsigned long long)len);
        }
        printf("\"\n");
        dataPos += len;
      } else {
        printf("ERROR: Not enough data (need %llu bytes, have %u)\n",
               (unsigned long long)len, size - dataPos);
      }
    } else if( serialType >= 12 && serialType%2 == 0 ){
      uint64_t len = (serialType - 12) / 2;
      if( dataPos + len <= size ){
        printf("BLOB(%llu bytes): ", (unsigned long long)len);
        uint32_t printLen = len < 64 ? len : 64;
        for(i=0; i<printLen; i++){
          printf("%02x", record[dataPos+i]);
          if((i+1)%4==0 && i+1<printLen) printf(" ");
        }
        if( len > 64 ) printf("...");
        printf("\n");
        dataPos += len;
      } else {
        printf("ERROR: Not enough data\n");
      }
    } else {
      /* Other integer types */
      uint32_t byteLen = 0;
      if( serialType == 5 ) byteLen = 6;
      else if( serialType == 6 ) byteLen = 8;
      else if( serialType == 7 ) byteLen = 8;

      if( byteLen > 0 && dataPos + byteLen <= size ){
        printf("(raw %u bytes): ", byteLen);
        for(i=0; i<byteLen; i++){
          printf("%02x ", record[dataPos+i]);
        }
        printf("\n");
        dataPos += byteLen;
      } else {
        printf("(unhandled serial type)\n");
      }
    }

    colNum++;
  }
  printf("\n");
}

/* Validate and process a leaf table cell */
static void processLeafCell(WalkContext *ctx, unsigned char *page,
                            uint32_t cellOffset, int headerOffset,
                            uint32_t pageNum, uint32_t cellNum) {
  uint64_t payloadSize, rowid;
  int n, valid = 1;
  uint32_t usableSize = ctx->pageSize - ctx->reservedSpace;

  if( cellOffset < headerOffset + 8 ){
    if( ctx->verbose ){
      printf("  Cell %u: INVALID cell offset %u (before page header end)\n",
             cellNum, cellOffset);
    }
    ctx->corruptCells++;
    return;
  }

  if( cellOffset >= usableSize ){
    if( ctx->verbose ){
      printf("  Cell %u: INVALID cell offset %u (beyond usable space %u)\n",
             cellNum, cellOffset, usableSize);
    }
    ctx->corruptCells++;
    return;
  }

  /* Read payload size and rowid */
  n = readVarint(&page[cellOffset], &payloadSize);
  if( n < 1 || n > 9 || cellOffset + n >= usableSize ){
    if( ctx->verbose ){
      printf("  Cell %u: CORRUPT payload size varint at offset %u\n",
             cellNum, cellOffset);
    }
    ctx->corruptCells++;
    return;
  }

  int m = readVarint(&page[cellOffset + n], &rowid);
  if( m < 1 || m > 9 || cellOffset + n + m >= usableSize ){
    if( ctx->verbose ){
      printf("  Cell %u: CORRUPT rowid varint at offset %u\n",
             cellNum, cellOffset + n);
    }
    ctx->corruptCells++;
    return;
  }

  /* Update statistics */
  ctx->cellsScanned++;
  if( ctx->cellsScanned == 1 || rowid < ctx->minRowid ){
    ctx->minRowid = rowid;
  }
  if( ctx->cellsScanned == 1 || rowid > ctx->maxRowid ){
    ctx->maxRowid = rowid;
  }

  /* Calculate local payload */
  uint32_t maxLocal = usableSize - 35;
  uint32_t minLocal = ((usableSize - 12) * 32 / 255) - 23;
  uint32_t local;

  if( payloadSize <= maxLocal ){
    local = payloadSize;
  } else {
    local = minLocal + ((payloadSize - minLocal) % (usableSize - 4));
  }

  /* Check if payload extends beyond page */
  if( cellOffset + n + m + local > usableSize ){
    if( ctx->verbose || ctx->findRowid == rowid ){
      printf("  Cell %u (rowid %llu): CORRUPT - payload extends beyond usable space\n",
             cellNum, (unsigned long long)rowid);
    }
    valid = 0;
    ctx->corruptCells++;
  }

  /* Check for overflow */
  int hasOverflow = (payloadSize > local);

  if( ctx->verbose ){
    printf("  Cell %u: rowid=%llu payload=%llu local=%u%s%s\n",
           cellNum, (unsigned long long)rowid, (unsigned long long)payloadSize,
           local, hasOverflow ? " OVERFLOW" : "",
           valid ? "" : " CORRUPT");
  }

  /* If this is the target rowid, dump it */
  if( ctx->findRowid != 0 && rowid == ctx->findRowid ){
    ctx->foundTarget = 1;
    printf("\n*** FOUND TARGET ROWID %llu ***\n", (unsigned long long)rowid);
    printf("Location: Page %u, Cell %u, Offset %u\n", pageNum, cellNum, cellOffset);
    printf("Payload size: %llu bytes\n", (unsigned long long)payloadSize);
    printf("Local payload: %u bytes\n", local);
    if( hasOverflow ){
      uint32_t overflowPgno = read32(&page[cellOffset + n + m + local]);
      printf("Overflow chain starts at page: %u\n", overflowPgno);
    }
    printf("Valid: %s\n", valid ? "YES" : "NO - CORRUPT");

    if( valid ){
      dumpRecord(&page[cellOffset + n + m], local, rowid);
    } else {
      printf("\nRecord is too corrupt to parse safely.\n");
      printf("Raw bytes at cell (first 128 bytes):\n");
      uint32_t dumpLen = 128;
      if( cellOffset + dumpLen > usableSize ) dumpLen = usableSize - cellOffset;
      for(uint32_t i=0; i<dumpLen; i++){
        if( i>0 && i%32==0 ) printf("\n");
        printf("%02x", page[cellOffset + i]);
        if((i+1)%4==0 && (i+1)%32!=0) printf(" ");
      }
      printf("\n");
    }
  }
}

/* Process a leaf page */
static void processLeafPage(WalkContext *ctx, uint32_t pgno, unsigned char *page) {
  int headerOffset = (pgno == 1) ? 100 : 0;
  uint8_t pageType = page[headerOffset + BTREE_HEADER_OFFSET_TYPE];
  uint32_t cellCount = read16(&page[headerOffset + BTREE_HEADER_OFFSET_CELL_COUNT]);
  uint32_t cellContent = read16(&page[headerOffset + BTREE_HEADER_OFFSET_CELL_CONTENT]);
  uint8_t fragmented = page[headerOffset + BTREE_HEADER_OFFSET_FRAGMENTED];

  ctx->leafPagesScanned++;

  if( ctx->verbose ){
    printf("\nLeaf page %u:\n", pgno);
    printf("  Type: 0x%02x\n", pageType);
    printf("  Cell count: %u\n", cellCount);
    printf("  Cell content area: %u\n", cellContent);
    printf("  Fragmented bytes: %u\n", fragmented);
  }

  /* Process each cell */
  for(uint32_t i=0; i<cellCount; i++){
    uint32_t cellOffset = read16(&page[headerOffset + 8 + i*2]);
    processLeafCell(ctx, page, cellOffset, headerOffset, pgno, i);
  }
}

/* Walk the btree */
static void walkBtree(WalkContext *ctx, uint32_t pgno) {
  /* Allocate local page buffer to avoid corruption during recursion */
  unsigned char *page = malloc(ctx->pageSize);
  if( !page ){
    fprintf(stderr, "Out of memory\n");
    return;
  }

  if( pgno == 0 || pgno > ctx->totalPages ){
    if( ctx->verbose )
      printf("DEBUG: Skipping page %u (out of range)\n", pgno);
    return;
  }

  if( readPage(ctx, pgno, page) != 0 ){
    fprintf(stderr, "ERROR: Failed to read page %u\n", pgno);
    free(page);
    return;
  }

  ctx->pagesScanned++;

  int headerOffset = (pgno == 1) ? 100 : 0;
  uint32_t usableSize = ctx->pageSize - ctx->reservedSpace;
  uint8_t pageType = page[headerOffset + BTREE_HEADER_OFFSET_TYPE];

  printf("DEBUG: Visiting page %u, type 0x%02x\n", pgno, pageType);
  if( pageType == BTREE_LEAF_TABLE ){
    processLeafPage(ctx, pgno, page);
  } else if( pageType == BTREE_INTERIOR_TABLE ){
    ctx->interiorPagesScanned++;
    uint32_t cellCount = read16(&page[headerOffset + BTREE_HEADER_OFFSET_CELL_COUNT]);

    if( ctx->verbose ){
      printf("\nInterior page %u: %u cells\n", pgno, cellCount);
    }

    /* Walk child pages */
    for(uint32_t i=0; i<cellCount; i++){
      uint32_t cellOffset = read16(&page[headerOffset + 12 + i*2]);
      if( cellOffset >= headerOffset + 12 && cellOffset < usableSize ){
        uint32_t childPgno = read32(&page[cellOffset]);
        if( childPgno > 0 && childPgno <= ctx->totalPages ){
          walkBtree(ctx, childPgno);
        }
      }
    }

    /* Walk rightmost child */
    uint32_t rightmost = read32(&page[headerOffset + BTREE_HEADER_OFFSET_RIGHTMOST]);
    walkBtree(ctx, rightmost);
  }
  free(page);
}

int main(int argc, char **argv) {
  WalkContext ctx;
  unsigned char header[SQLITE_HEADER_SIZE];
  char *dbFile = NULL;
  char *tableName = NULL;
  uint32_t rootPage = 0;
  int i;

  memset(&ctx, 0, sizeof(ctx));

  /* Parse arguments */
  if( argc < 3 ){
    fprintf(stderr, "Usage: %s DATABASE TABLE [OPTIONS]\n", argv[0]);
    fprintf(stderr, "\n");
    fprintf(stderr, "Options:\n");
    fprintf(stderr, "  --find-rowid=ROWID   Find and dump a specific rowid\n");
    fprintf(stderr, "  --verbose            Print info about every page\n");
    fprintf(stderr, "  --validate           Enable validation (always on)\n");
    fprintf(stderr, "\n");
    fprintf(stderr, "Examples:\n");
    fprintf(stderr, "  %s mydb.db MyTable --find-rowid=12345\n", argv[0]);
    fprintf(stderr, "  %s mydb.db MyTable --verbose\n", argv[0]);
    return 1;
  }

  dbFile = argv[1];
  tableName = argv[2];
  ctx.validate = 1; /* Always validate */

  for(i=3; i<argc; i++){
    if( strncmp(argv[i], "--find-rowid=", 13) == 0 ){
      ctx.findRowid = strtoull(argv[i] + 13, NULL, 10);
    } else if( strcmp(argv[i], "--verbose") == 0 ){
      ctx.verbose = 1;
    } else if( strcmp(argv[i], "--validate") == 0 ){
      ctx.validate = 1;
    } else {
      fprintf(stderr, "Unknown option: %s\n", argv[i]);
      return 1;
    }
  }

  /* Open database */
  ctx.db = fopen(dbFile, "rb");
  if( !ctx.db ){
    fprintf(stderr, "Cannot open %s\n", dbFile);
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
    fprintf(stderr, "%s is not a valid SQLite database\n", dbFile);
    fclose(ctx.db);
    return 1;
  }

  /* Parse header */
  ctx.pageSize = readPageSize(header);
  ctx.reservedSpace = header[OFFSET_RESERVED_SPACE];
  ctx.totalPages = read32(&header[28]);

  /* Allocate page buffer */
  ctx.pageBuf = malloc(ctx.pageSize);
  if( !ctx.pageBuf ){
    fprintf(stderr, "Out of memory\n");
    fclose(ctx.db);
    return 1;
  }

  printf("=== SQLite Table Walker ===\n");
  printf("Database: %s\n", dbFile);
  printf("Table: %s\n", tableName);
  printf("Page size: %u bytes\n", ctx.pageSize);
  printf("Reserved space: %u bytes\n", ctx.reservedSpace);
  printf("Total pages: %u\n", ctx.totalPages);
  if( ctx.findRowid != 0 ){
    printf("Searching for rowid: %llu\n", (unsigned long long)ctx.findRowid);
  }
  printf("\n");

  /* Find table root page - we need to pass it as an extra arg or query SQLite */
  /* For now, require it as the 3rd arg if not using options */
  /* Better: use sqlite3 API or parse schema ourselves */

  /* Simple approach: let user specify root page or we query it */
  char cmd[512];
  snprintf(cmd, sizeof(cmd),
           "sqlite3 \"%s\" \"SELECT rootpage FROM sqlite_master WHERE name='%s'\"",
           dbFile, tableName);

  FILE *fp = popen(cmd, "r");
  if( fp ){
    if( fscanf(fp, "%u", &rootPage) != 1 ){
      fprintf(stderr, "Table '%s' not found\n", tableName);
      pclose(fp);
      free(ctx.pageBuf);
      fclose(ctx.db);
      return 1;
    }
    pclose(fp);
  } else {
    fprintf(stderr, "Cannot query schema (sqlite3 command not available)\n");
    fprintf(stderr, "Please provide root page as: --root=PAGENUM\n");
    free(ctx.pageBuf);
    fclose(ctx.db);
    return 1;
  }

  printf("Table root page: %u\n\n", rootPage);
  printf("Walking table...\n\n");

  /* Walk the table */
  walkBtree(&ctx, rootPage);

  /* Print summary */
  printf("\n=== SUMMARY ===\n");
  printf("Pages scanned: %u\n", ctx.pagesScanned);
  printf("  Interior pages: %u\n", ctx.interiorPagesScanned);
  printf("  Leaf pages: %u\n", ctx.leafPagesScanned);
  printf("Cells scanned: %u\n", ctx.cellsScanned);
  printf("Corrupt cells: %u\n", ctx.corruptCells);
  if( ctx.cellsScanned > 0 ){
    printf("Rowid range: %llu .. %llu\n",
           (unsigned long long)ctx.minRowid,
           (unsigned long long)ctx.maxRowid);
  }

  if( ctx.findRowid != 0 ){
    printf("\nTarget rowid %llu: %s\n",
           (unsigned long long)ctx.findRowid,
           ctx.foundTarget ? "FOUND" : "NOT FOUND");
  }

  free(ctx.pageBuf);
  fclose(ctx.db);
  return ctx.foundTarget || ctx.findRowid == 0 ? 0 : 1;
}
