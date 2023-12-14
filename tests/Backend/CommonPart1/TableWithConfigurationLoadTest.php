<?php

namespace Keboola\Test\Backend\CommonPart1;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\Test\Backend\TableWithConfigurationUtils;
use Keboola\Test\ClientProvider\ClientProvider;
use Keboola\Test\StorageApiTestCase;
use Keboola\Test\Utils\EventsQueryBuilder;

class TableWithConfigurationLoadTest extends StorageApiTestCase
{
    use TableWithConfigurationUtils;

    private Client $client;

    private ClientProvider $clientProvider;

    private Components $componentsClient;

    public function setUp(): void
    {
        parent::setUp();

        $this->checkFeatureAndBackend();

        // init buckets
        $this->initEmptyTestBucketsForParallelTests();

        $this->clientProvider = new ClientProvider($this);
        $this->client = $this->clientProvider->createClientForCurrentTest();

        $this->assertComponentExists();

        $this->configId = sha1($this->generateDescriptionForTestObject());

        $this->dropTableAndConfiguration($this->configId);

        $this->initEvents($this->client);
    }

    /**
     * @throws ClientException
     */
    private function loadTableFromFile(
        string $tableId,
        string $csvFilePath = __DIR__ . '/../../_data/languages.csv',
        array $writeTableOptions = []
    ): void {
        $csvFile = new CsvFile($csvFilePath);
        $fileId = $this->_client->uploadFile(
            $csvFile->getPathname(),
            (new FileUploadOptions())
                ->setNotify(false)
                ->setIsPublic(false)
                ->setCompress(true)
                ->setTags(['table-import']),
        );

        $this->_client->writeTableAsyncDirect($tableId, [
            'dataFileId' => $fileId,
        ] + $writeTableOptions);
    }

    public function testLoadFromFileToTable(): void
    {
        $tableName = 'custom-table-1';

        // paste output from CustomQuery component + replace `\n` with `\\n`
        $json = /** @lang JSON */
            <<<JSON
{
  "output": {
    "queries": [
      {
        "sql": "CREATE TABLE {{ id(stageSchemaName) }}.{{ id(stageTableName) }} ([id] NVARCHAR(4000), [NAME] NVARCHAR(4000)) WITH (DISTRIBUTION = ROUND_ROBIN,CLUSTERED COLUMNSTORE INDEX)",
        "description": ""
      },
      {
        "sql": "COPY INTO {{ id(stageSchemaName) }}.{{ id(stageTableName) }}\\nFROM {{ listFiles(sourceFiles) }}\\nWITH (\\n    FILE_TYPE='CSV',\\n    CREDENTIAL=(IDENTITY='Managed Identity'),\\n    FIELDQUOTE='\\"',\\n    FIELDTERMINATOR=',',\\n    ENCODING = 'UTF8',\\n    \\n    IDENTITY_INSERT = 'OFF'\\n    ,FIRSTROW=2\\n)",
        "description": ""
      },
      {
        "sql": "CREATE TABLE {{ id(schemaName) }}.{{ id(tableName ~ rand ~ '_tmp') }} WITH (DISTRIBUTION=ROUND_ROBIN,CLUSTERED COLUMNSTORE INDEX) AS SELECT a.[id],a.[NAME],a.[_timestamp] FROM (SELECT COALESCE([id], '') AS [id],COALESCE([NAME], '') AS [NAME],CAST({{ q(timestamp) }} as DATETIME2) AS [_timestamp], ROW_NUMBER() OVER (PARTITION BY [id] ORDER BY [id]) AS \\"_row_number_\\" FROM {{ id(stageSchemaName) }}.{{ id(stageTableName) }}) AS a WHERE a.\\"_row_number_\\" = 1",
        "description": ""
      },
      {
        "sql": "RENAME OBJECT {{ id(schemaName) }}.{{ id(tableName) }} TO {{ id(tableName ~ rand ~ '_tmp_rename') }}",
        "description": ""
      },
      {
        "sql": "RENAME OBJECT {{ id(schemaName) }}.{{ id(tableName ~ rand ~ '_tmp') }} TO {{ id(tableName) }}",
        "description": ""
      },
      {
        "sql": "DROP TABLE {{ id(schemaName) }}.{{ id(tableName ~ rand ~ '_tmp_rename') }}",
        "description": ""
      },
      {
        "sql": "IF OBJECT_ID (N'{{ id(schemaName) }}.{{ id(tableName ~ rand ~ '_tmp') }}', N'U') IS NOT NULL DROP TABLE {{ id(schemaName) }}.{{ id(tableName ~ rand ~ '_tmp') }}",
        "description": ""
      },
      {
        "sql": "IF OBJECT_ID (N'{{ id(schemaName) }}.{{ id(tableName ~ rand ~ '_tmp_rename') }}', N'U') IS NOT NULL DROP TABLE {{ id(schemaName) }}.{{ id(tableName ~ rand ~ '_tmp_rename') }}",
        "description": ""
      }
    ]
  }
}
JSON;
        [$tableId,] = $this->createTableWithConfiguration($json, $tableName, 'importFromFileFull');

        $this->loadTableFromFile($tableId);

        $table = $this->_client->getTable($tableId);

        $this->assertEquals(['id', 'NAME'], $table['columns']);
        $this->assertSame(5, $table['rowsCount']);
        $this->assertTableColumnMetadata([
            'id' => [
                'KBC.datatype.type' => 'INT',
                'KBC.datatype.nullable' => '1',
                'KBC.datatype.basetype' => 'INTEGER',
            ],
            'NAME' => [
                'KBC.datatype.type' => 'VARCHAR',
                'KBC.datatype.nullable' => '1',
                'KBC.datatype.basetype' => 'STRING',
                'KBC.datatype.length' => '100',
            ],
        ], $table);

        // check events
        $assertCallback = function ($events) {
            $this->assertCount(8, $events);
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.tableWithConfigurationImportQuery')
            ->setTokenId($this->tokenId)
            ->setObjectId($tableId);
        $this->assertEventWithRetries($this->client, $assertCallback, $query);

        $assertCallback = function ($events) {
            $this->assertCount(1, $events);
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.tableImportDone')
            ->setTokenId($this->tokenId)
            ->setObjectId($tableId);
        $this->assertEventWithRetries($this->client, $assertCallback, $query);
    }

    public function testLoadFromFileToTableOnError(): void
    {
        $tableName = 'custom-table-1';

        /**
         * This configuration will:
         * - create two new tables tmp_test1,tmp_test2
         * - clear tables (tmp_test1,tmp_test2) if exist in case of some failures
         *
         * Purpose is to test if tables are not exists, this would fail if table tmp_test1 or tmp_test2 exists
         * Table existing in bucket means that cleanup or onError doesn't work in other configurations
         */
        $jsonTest = /** @lang JSON */
            <<<JSON
{
  "output": {
    "queries": [
      {
        "sql": "CREATE TABLE {{ id(schemaName) }}.{{ id('tmp_test1') }} ([id] NVARCHAR(4000), [NAME] NVARCHAR(4000)) WITH (DISTRIBUTION = ROUND_ROBIN,CLUSTERED COLUMNSTORE INDEX)",
        "description": ""
      },
      {
        "sql": "CREATE TABLE {{ id(schemaName) }}.{{ id('tmp_test2') }} ([id] NVARCHAR(4000), [NAME] NVARCHAR(4000)) WITH (DISTRIBUTION = ROUND_ROBIN,CLUSTERED COLUMNSTORE INDEX)",
        "description": ""
      },
      {
        "sql": "IF OBJECT_ID (N'{{ id(schemaName) }}.{{ id('tmp_test1') }}', N'U') IS NOT NULL DROP TABLE {{ id(schemaName) }}.{{ id('tmp_test1') }}",
        "description": ""
      },
      {
        "sql": "IF OBJECT_ID (N'{{ id(schemaName) }}.{{ id('tmp_test2') }}', N'U') IS NOT NULL DROP TABLE {{ id(schemaName) }}.{{ id('tmp_test2') }}",
        "description": ""
      }
    ],
    "onError": [],
    "cleanup": []
  }
}
JSON;

        $testConfig = json_decode(
            $jsonTest,
            true,
            512,
            JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT,
        )['output'];

        /**
         * This configuration will:
         * - clear tables (tmp_test1,tmp_test2) if exist in case of some failures
         * - create two new tables tmp_test1,tmp_test2
         * - run query which fails
         * - run cleanup for tables tmp_test1,tmp_test2
         *
         * there should not be any tables left
         */
        $jsonWithCleanup = /** @lang JSON */
            <<<JSON
{
  "output": {
    "queries": [
      {
        "sql": "IF OBJECT_ID (N'{{ id(schemaName) }}.{{ id('tmp_test1') }}', N'U') IS NOT NULL DROP TABLE {{ id(schemaName) }}.{{ id('tmp_test1') }}",
        "description": ""
      },
      {
        "sql": "IF OBJECT_ID (N'{{ id(schemaName) }}.{{ id('tmp_test2') }}', N'U') IS NOT NULL DROP TABLE {{ id(schemaName) }}.{{ id('tmp_test2') }}",
        "description": ""
      },
      {
        "sql": "CREATE TABLE {{ id(schemaName) }}.{{ id('tmp_test1') }} ([id] NVARCHAR(4000), [NAME] NVARCHAR(4000)) WITH (DISTRIBUTION = ROUND_ROBIN,CLUSTERED COLUMNSTORE INDEX)",
        "description": ""
      },
      {
        "sql": "CREATE TABLE {{ id(schemaName) }}.{{ id('tmp_test2') }} ([id] NVARCHAR(4000), [NAME] NVARCHAR(4000)) WITH (DISTRIBUTION = ROUND_ROBIN,CLUSTERED COLUMNSTORE INDEX)",
        "description": ""
      },
      {
        "sql": "FAIL THIS",
        "description": ""
      }
    ],
    "onError": [],
    "cleanup": [
      {
        "sql": "IF OBJECT_ID (N'{{ id(schemaName) }}.{{ id('tmp_test1') }}', N'U') IS NOT NULL DROP TABLE {{ id(schemaName) }}.{{ id('tmp_test1') }}",
        "description": ""
      },
      {
        "sql": "IF OBJECT_ID (N'{{ id(schemaName) }}.{{ id('tmp_test2') }}', N'U') IS NOT NULL DROP TABLE {{ id(schemaName) }}.{{ id('tmp_test2') }}",
        "description": ""
      }
    ]
  }
}
JSON;
        [$tableId, $configuration] = $this->createTableWithConfiguration($jsonWithCleanup, $tableName, 'importFromFileFull');

        try {
            $this->loadTableFromFile($tableId);
            $this->fail('import should fail');
        } catch (ClientException $e) {
            $this->assertStringStartsWith('Execution of custom table query failed because of', $e->getMessage());
        }

        /**
         * Test if tables are cleared
         */
        $configuration->setConfiguration([
            'parameters' => [
                'migrations' => [/** we don't care about migrations they can be empty */],
                'queriesOverride' => [
                    'importFromFileFull' => $testConfig,
                ],
            ],
        ]);
        $this->componentsClient->updateConfiguration($configuration);
        $this->loadTableFromFile($tableId); // now should succeed as tables were cleared in cleanup

        /**
         * This configuration will:
         * - clear tables (tmp_test1,tmp_test2) if exist in case of some failures
         * - create two new tables tmp_test1,tmp_test2
         * - run query which fails
         * - run onError for tables tmp_test1,tmp_test2
         *
         * there should not be any tables left
         */
        $jsonWithOnError = /** @lang JSON */
            <<<JSON
{
  "output": {
    "queries": [
      {
        "sql": "IF OBJECT_ID (N'{{ id(schemaName) }}.{{ id('tmp_test1') }}', N'U') IS NOT NULL DROP TABLE {{ id(schemaName) }}.{{ id('tmp_test1') }}",
        "description": ""
      },
      {
        "sql": "IF OBJECT_ID (N'{{ id(schemaName) }}.{{ id('tmp_test2') }}', N'U') IS NOT NULL DROP TABLE {{ id(schemaName) }}.{{ id('tmp_test2') }}",
        "description": ""
      },
      {
        "sql": "CREATE TABLE {{ id(schemaName) }}.{{ id('tmp_test1') }} ([id] NVARCHAR(4000), [NAME] NVARCHAR(4000)) WITH (DISTRIBUTION = ROUND_ROBIN,CLUSTERED COLUMNSTORE INDEX)",
        "description": ""
      },
      {
        "sql": "CREATE TABLE {{ id(schemaName) }}.{{ id('tmp_test2') }} ([id] NVARCHAR(4000), [NAME] NVARCHAR(4000)) WITH (DISTRIBUTION = ROUND_ROBIN,CLUSTERED COLUMNSTORE INDEX)",
        "description": ""
      },
      {
        "sql": "FAIL THIS",
        "description": ""
      }
    ],
    "onError": [
      {
        "sql": "IF OBJECT_ID (N'{{ id(schemaName) }}.{{ id('tmp_test1') }}', N'U') IS NOT NULL DROP TABLE {{ id(schemaName) }}.{{ id('tmp_test1') }}",
        "description": ""
      },
      {
        "sql": "IF OBJECT_ID (N'{{ id(schemaName) }}.{{ id('tmp_test2') }}', N'U') IS NOT NULL DROP TABLE {{ id(schemaName) }}.{{ id('tmp_test2') }}",
        "description": ""
      }
    ],
    "cleanup": []
  }
}
JSON;

        $configOnError = json_decode(
            $jsonWithOnError,
            true,
            512,
            JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT,
        )['output'];

        $configuration->setConfiguration([
            'parameters' => [
                'migrations' => [/** we don't care about migrations they can be empty */],
                'queriesOverride' => [
                    'importFromFileFull' => $configOnError,
                ],
            ],
        ]);
        $this->componentsClient->updateConfiguration($configuration);

        try {
            $this->loadTableFromFile($tableId);
            $this->fail('import should fail');
        } catch (ClientException $e) {
            $this->assertStringStartsWith('Execution of custom table query failed because of', $e->getMessage());
        }

        /**
         * Test if tables are cleared
         */
        $configuration->setConfiguration([
            'parameters' => [
                'migrations' => [/** we don't care about migrations they can be empty */],
                'queriesOverride' => [
                    'importFromFileFull' => $testConfig,
                ],
            ],
        ]);
        $this->componentsClient->updateConfiguration($configuration);
        $this->loadTableFromFile($tableId); // now should succeed as tables were cleared in cleanup
    }

    public function testLoadIncrementalFromFileToTable(): void
    {
        $tableName = 'custom-table-2';

        // put output from CustomQuery component + replace `\n` with `\\n`
        $json = /** @lang JSON */
            <<<JSON
{
  "output": {
    "queries": [
      {
        "sql": "CREATE TABLE {{ id(stageSchemaName) }}.{{ id(stageTableName) }} ([id] NVARCHAR(4000), [NAME] NVARCHAR(4000)) WITH (DISTRIBUTION = ROUND_ROBIN,CLUSTERED COLUMNSTORE INDEX)",
        "description": ""
      },
      {
        "sql": "COPY INTO {{ id(stageSchemaName) }}.{{ id(stageTableName) }}\\nFROM {{ listFiles(sourceFiles) }}\\nWITH (\\n    FILE_TYPE='CSV',\\n    CREDENTIAL=(IDENTITY='Managed Identity'),\\n    FIELDQUOTE='\"',\\n    FIELDTERMINATOR=',',\\n    ENCODING = 'UTF8',\\n    \\n    IDENTITY_INSERT = 'OFF'\\n    ,FIRSTROW=2\\n)",
        "description": ""
      },
      {
        "sql": "CREATE TABLE {{ id(stageSchemaName) }}.{{ id(stageTableName ~ rand ~ '_tmp') }} ([id] NVARCHAR(4000), [NAME] NVARCHAR(4000)) WITH (DISTRIBUTION = ROUND_ROBIN,CLUSTERED COLUMNSTORE INDEX)",
        "description": ""
      },
      {
        "sql": "BEGIN TRANSACTION",
        "description": ""
      },
      {
        "sql": "UPDATE {{ id(schemaName) }}.{{ id(tableName) }} SET [NAME] = COALESCE([src].[NAME], ''), [_timestamp] = {{ q(timestamp) }} FROM {{ id(stageSchemaName) }}.{{ id(stageTableName) }} AS [src] WHERE {{ id(schemaName) }}.{{ id(tableName) }}.[id] = [src].[id] AND (COALESCE(CAST({{ id(schemaName) }}.{{ id(tableName) }}.[NAME] AS NVARCHAR), '') != COALESCE([src].[NAME], '')) ",
        "description": ""
      },
      {
        "sql": "DELETE {{ id(stageSchemaName) }}.{{ id(stageTableName) }} WHERE EXISTS (SELECT * FROM {{ id(schemaName) }}.{{ id(tableName) }} WHERE {{ id(schemaName) }}.{{ id(tableName) }}.[id] = {{ id(stageSchemaName) }}.{{ id(stageTableName) }}.[id])",
        "description": ""
      },
      {
        "sql": "INSERT INTO {{ id(stageSchemaName) }}.{{ id(stageTableName ~ rand ~ '_tmp') }} ([id], [NAME]) SELECT a.[id],a.[NAME] FROM (SELECT [id], [NAME], ROW_NUMBER() OVER (PARTITION BY [id] ORDER BY [id]) AS \\"_row_number_\\" FROM {{ id(stageSchemaName) }}.{{ id(stageTableName) }}) AS a WHERE a.\\"_row_number_\\" = 1",
        "description": ""
      },
      {
        "sql": "INSERT INTO {{ id(schemaName) }}.{{ id(tableName) }} ([id], [NAME], [_timestamp]) (SELECT CAST(COALESCE([id], '') as NVARCHAR) AS [id],CAST(COALESCE([NAME], '') as NVARCHAR) AS [NAME],{{ q(timestamp) }} FROM {{ id(stageSchemaName) }}.{{ id(stageTableName ~ rand ~ '_tmp') }} AS [src])",
        "description": ""
      },
      {
        "sql": "COMMIT",
        "description": ""
      }
    ]
  }
}
JSON;

        [$tableId,] = $this->createTableWithConfiguration($json, $tableName, 'importFromFileIncremental', [
            [
                'sql' => /** @lang TSQL */
                    <<<SQL
                    CREATE TABLE {{ id(bucketName) }}.{{ id(tableName) }} ([id] INTEGER, [NAME] VARCHAR(100), [_timestamp] DATETIME2)
                    SQL,
                'description' => 'first ever',
            ],
            [
                'sql' => /** @lang TSQL */
                    <<<SQL
                    INSERT INTO {{ id(bucketName) }}.{{ id(tableName) }} ([id], [NAME]) 
                        SELECT 0, '- unchecked -' UNION ALL
                        SELECT 26, 'czech' UNION ALL
                        SELECT 1, 'english' UNION ALL
                        SELECT 11, 'finnish' UNION ALL
                        SELECT 24, 'french';
                    SQL,
            ],
        ]);

        $this->loadTableFromFile(
            $tableId,
            __DIR__ . '/../../_data/languages.increment.csv',
            [
                'incremental' => true,
            ],
        );

        $table = $this->_client->getTable($tableId);

        $this->assertEquals(['id', 'NAME'], $table['columns']);
        $this->assertSame(6, $table['rowsCount']);
        $tableData = Client::parseCsv($this->_client->getTableDataPreview($tableId));
        $this->assertArrayEqualsSorted([
            ['id' => '24', 'NAME' => 'french',],
            ['id' => '1', 'NAME' => 'english',],
            ['id' => '25', 'NAME' => 'russian',],
            ['id' => '0', 'NAME' => '- unchecked -',],
            ['id' => '11', 'NAME' => 'finnish',],
            ['id' => '26', 'NAME' => 'slovak',],
        ], $tableData, 'id');
        $this->assertTableColumnMetadata([
            'id' => [
                'KBC.datatype.type' => 'INT',
                'KBC.datatype.nullable' => '1',
                'KBC.datatype.basetype' => 'INTEGER',
            ],
            'NAME' => [
                'KBC.datatype.type' => 'VARCHAR',
                'KBC.datatype.nullable' => '1',
                'KBC.datatype.basetype' => 'STRING',
                'KBC.datatype.length' => '100',
            ],
        ], $table);

        // check events
        $assertCallback = function ($events) {
            $this->assertCount(9, $events);
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.tableWithConfigurationImportQuery')
            ->setTokenId($this->tokenId)
            ->setObjectId($tableId);
        $this->assertEventWithRetries($this->client, $assertCallback, $query);

        $assertCallback = function ($events) {
            $this->assertCount(1, $events);
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.tableImportDone')
            ->setTokenId($this->tokenId)
            ->setObjectId($tableId);
        $this->assertEventWithRetries($this->client, $assertCallback, $query);
    }
}
