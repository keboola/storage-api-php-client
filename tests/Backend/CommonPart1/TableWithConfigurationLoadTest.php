<?php

namespace Keboola\Test\Backend\CommonPart1;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\Test\Backend\TableWithConfigurationUtils;
use Keboola\Test\ClientProvider\ClientProvider;
use Keboola\Test\StorageApiTestCase;
use Keboola\Test\Utils\EventTesterUtils;

class TableWithConfigurationLoadTest extends StorageApiTestCase
{
    use EventTesterUtils;
    use TableWithConfigurationUtils;

    private Client $client;

    private ClientProvider $clientProvider;

    private Components $componentsClient;

    /**
     * @throws ClientException
     */
    private function loadTable(string $tableId): void
    {
        $csvFile = new CsvFile(__DIR__ . '/../../_data/languages.csv');
        $fileId = $this->_client->uploadFile(
            $csvFile->getPathname(),
            (new FileUploadOptions())
                ->setNotify(false)
                ->setIsPublic(false)
                ->setCompress(true)
                ->setTags(['table-import'])
        );

        $this->_client->writeTableAsyncDirect($tableId, [
            'dataFileId' => $fileId,
        ]);
    }

    /**
     * @return array{0: string, 1: Configuration}
     * @throws \JsonException
     */
    private function createTableWithConfiguration(string $json, string $tableName): array
    {
        /** @var array{output:array} $jsonDecoded */
        $jsonDecoded = json_decode(
            $json,
            true,
            512,
            JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT
        );

        $queriesOverride = [];
        $queriesOverride['ingestionFullLoad'] = $jsonDecoded['output'];

        $configuration = (new Configuration())
            ->setComponentId(StorageApiTestCase::CUSTOM_QUERY_MANAGER_COMPONENT_ID)
            ->setConfigurationId($this->configId)
            ->setName($this->configId)
            ->setConfiguration([
                'migrations' => [
                    [
                        'sql' => /** @lang TSQL */ <<<SQL
CREATE TABLE {{ id(bucketName) }}.{{ id(tableName) }} ([id] INTEGER, [NAME] VARCHAR(100))
SQL,
                        'description' => 'first ever',
                    ],
                ],
                'queriesOverride' => $queriesOverride,
            ]);

        return [
            $this->prepareTableWithConfiguration($tableName, $configuration),
            $configuration,
        ];
    }

    public function setUp(): void
    {
        parent::setUp();

        // check feature
        $token = $this->_client->verifyToken();
        if (!in_array('tables-with-configuration', $token['owner']['features'])) {
            $this->markTestSkipped(sprintf('Creating tables from configurations feature is not enabled for project "%s"', $token['owner']['id']));
        }

        if ($token['owner']['defaultBackend'] !== self::BACKEND_SYNAPSE) {
            self::markTestSkipped(sprintf(
                'Backend "%s" is not supported tables with configuration',
                $token['owner']['defaultBackend']
            ));
        }

        // init buckets
        $this->initEmptyTestBucketsForParallelTests();

        $this->clientProvider = new ClientProvider($this);
        $this->client = $this->clientProvider->createClientForCurrentTest();

        $this->assertComponentExists();

        $this->configId = sha1($this->generateDescriptionForTestObject());

        $this->dropTableAndConfiguration($this->configId);

        $this->initEvents($this->client);
    }

    public function testLoadFromFileToTable(): void
    {
        $tableName = 'custom-table-1';

        $json = /** @lang JSON */
            <<<JSON
{
  "action": "generate",
  "backend": "synapse",
  "operation": "importFull",
  "source": "fileAbs",
  "columns": [
    "id",
    "NAME"
  ],
  "primaryKeys": [
    "id"
  ],
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
        "sql": "CREATE TABLE {{ id(destSchemaName) }}.{{ id(destTableName ~ rand ~ '_tmp') }} WITH (DISTRIBUTION=ROUND_ROBIN,CLUSTERED COLUMNSTORE INDEX) AS SELECT a.[id],a.[NAME] FROM (SELECT COALESCE([id], '') AS [id],COALESCE([NAME], '') AS [NAME], ROW_NUMBER() OVER (PARTITION BY [id] ORDER BY [id]) AS \"_row_number_\" FROM {{ id(stageSchemaName) }}.{{ id(stageTableName) }}) AS a WHERE a.\"_row_number_\" = 1",
        "description": ""
      },
      {
        "sql": "RENAME OBJECT {{ id(destSchemaName) }}.{{ id(destTableName) }} TO {{ id(destTableName ~ rand ~ '_tmp_rename') }}",
        "description": ""
      },
      {
        "sql": "RENAME OBJECT {{ id(destSchemaName) }}.{{ id(destTableName ~ rand ~ '_tmp') }} TO {{ id(destTableName) }}",
        "description": ""
      },
      {
        "sql": "DROP TABLE {{ id(destSchemaName) }}.{{ id(destTableName ~ rand ~ '_tmp_rename') }}",
        "description": ""
      },
      {
        "sql": "IF OBJECT_ID (N'{{ id(destSchemaName) }}.{{ id(destTableName ~ rand ~ '_tmp') }}', N'U') IS NOT NULL DROP TABLE {{ id(destSchemaName) }}.{{ id(destTableName ~ rand ~ '_tmp') }}",
        "description": ""
      },
      {
        "sql": "IF OBJECT_ID (N'{{ id(destSchemaName) }}.{{ id(destTableName ~ rand ~ '_tmp_rename') }}', N'U') IS NOT NULL DROP TABLE {{ id(destSchemaName) }}.{{ id(destTableName ~ rand ~ '_tmp_rename') }}",
        "description": ""
      }
    ]
  }
}
JSON;
        [$tableId,] = $this->createTableWithConfiguration($json, $tableName);

        $this->loadTable($tableId);

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
        $events = $this->listEventsFilteredByName($this->client, 'storage.tableWithConfigurationImportQuery', $tableId, 50);
        $this->assertCount(8, $events);

        $events = $this->listEventsFilteredByName($this->client, 'storage.tableImportDone', $tableId, 10);
        $this->assertCount(1, $events);
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
  "action": "generate",
  "backend": "synapse",
  "operation": "importFull",
  "source": "fileAbs",
  "columns": [
    "id",
    "NAME"
  ],
  "primaryKeys": [
    "id"
  ],
  "output": {
    "queries": [
      {
        "sql": "CREATE TABLE {{ id(destSchemaName) }}.{{ id('tmp_test1') }} ([id] NVARCHAR(4000), [NAME] NVARCHAR(4000)) WITH (DISTRIBUTION = ROUND_ROBIN,CLUSTERED COLUMNSTORE INDEX)",
        "description": ""
      },
      {
        "sql": "CREATE TABLE {{ id(destSchemaName) }}.{{ id('tmp_test2') }} ([id] NVARCHAR(4000), [NAME] NVARCHAR(4000)) WITH (DISTRIBUTION = ROUND_ROBIN,CLUSTERED COLUMNSTORE INDEX)",
        "description": ""
      },
      {
        "sql": "IF OBJECT_ID (N'{{ id(destSchemaName) }}.{{ id('tmp_test1') }}', N'U') IS NOT NULL DROP TABLE {{ id(destSchemaName) }}.{{ id('tmp_test1') }}",
        "description": ""
      },
      {
        "sql": "IF OBJECT_ID (N'{{ id(destSchemaName) }}.{{ id('tmp_test2') }}', N'U') IS NOT NULL DROP TABLE {{ id(destSchemaName) }}.{{ id('tmp_test2') }}",
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
            JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT
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
  "action": "generate",
  "backend": "synapse",
  "operation": "importFull",
  "source": "fileAbs",
  "columns": [
    "id",
    "NAME"
  ],
  "primaryKeys": [
    "id"
  ],
  "output": {
    "queries": [
      {
        "sql": "IF OBJECT_ID (N'{{ id(destSchemaName) }}.{{ id('tmp_test1') }}', N'U') IS NOT NULL DROP TABLE {{ id(destSchemaName) }}.{{ id('tmp_test1') }}",
        "description": ""
      },
      {
        "sql": "IF OBJECT_ID (N'{{ id(destSchemaName) }}.{{ id('tmp_test2') }}', N'U') IS NOT NULL DROP TABLE {{ id(destSchemaName) }}.{{ id('tmp_test2') }}",
        "description": ""
      },
      {
        "sql": "CREATE TABLE {{ id(destSchemaName) }}.{{ id('tmp_test1') }} ([id] NVARCHAR(4000), [NAME] NVARCHAR(4000)) WITH (DISTRIBUTION = ROUND_ROBIN,CLUSTERED COLUMNSTORE INDEX)",
        "description": ""
      },
      {
        "sql": "CREATE TABLE {{ id(destSchemaName) }}.{{ id('tmp_test2') }} ([id] NVARCHAR(4000), [NAME] NVARCHAR(4000)) WITH (DISTRIBUTION = ROUND_ROBIN,CLUSTERED COLUMNSTORE INDEX)",
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
        "sql": "IF OBJECT_ID (N'{{ id(destSchemaName) }}.{{ id('tmp_test1') }}', N'U') IS NOT NULL DROP TABLE {{ id(destSchemaName) }}.{{ id('tmp_test1') }}",
        "description": ""
      },
      {
        "sql": "IF OBJECT_ID (N'{{ id(destSchemaName) }}.{{ id('tmp_test2') }}', N'U') IS NOT NULL DROP TABLE {{ id(destSchemaName) }}.{{ id('tmp_test2') }}",
        "description": ""
      }
    ]
  }
}
JSON;
        [$tableId, $configuration] = $this->createTableWithConfiguration($jsonWithCleanup, $tableName);

        try {
            $this->loadTable($tableId);
            $this->fail('import should fail');
        } catch (ClientException $e) {
            $this->assertStringStartsWith('Execution of custom table query failed because of', $e->getMessage());
        }

        /**
         * Test if tables are cleared
         */
        $configuration->setConfiguration([
            'migrations' => [/** we don't care about migrations they can be empty */],
            'queriesOverride' => [
                'ingestionFullLoad' => $testConfig,
            ],
        ]);
        $this->componentsClient->updateConfiguration($configuration);
        $this->loadTable($tableId); // now should succeed as tables were cleared in cleanup

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
  "action": "generate",
  "backend": "synapse",
  "operation": "importFull",
  "source": "fileAbs",
  "columns": [
    "id",
    "NAME"
  ],
  "primaryKeys": [
    "id"
  ],
  "output": {
    "queries": [
      {
        "sql": "IF OBJECT_ID (N'{{ id(destSchemaName) }}.{{ id('tmp_test1') }}', N'U') IS NOT NULL DROP TABLE {{ id(destSchemaName) }}.{{ id('tmp_test1') }}",
        "description": ""
      },
      {
        "sql": "IF OBJECT_ID (N'{{ id(destSchemaName) }}.{{ id('tmp_test2') }}', N'U') IS NOT NULL DROP TABLE {{ id(destSchemaName) }}.{{ id('tmp_test2') }}",
        "description": ""
      },
      {
        "sql": "CREATE TABLE {{ id(destSchemaName) }}.{{ id('tmp_test1') }} ([id] NVARCHAR(4000), [NAME] NVARCHAR(4000)) WITH (DISTRIBUTION = ROUND_ROBIN,CLUSTERED COLUMNSTORE INDEX)",
        "description": ""
      },
      {
        "sql": "CREATE TABLE {{ id(destSchemaName) }}.{{ id('tmp_test2') }} ([id] NVARCHAR(4000), [NAME] NVARCHAR(4000)) WITH (DISTRIBUTION = ROUND_ROBIN,CLUSTERED COLUMNSTORE INDEX)",
        "description": ""
      },
      {
        "sql": "FAIL THIS",
        "description": ""
      }
    ],
    "onError": [
      {
        "sql": "IF OBJECT_ID (N'{{ id(destSchemaName) }}.{{ id('tmp_test1') }}', N'U') IS NOT NULL DROP TABLE {{ id(destSchemaName) }}.{{ id('tmp_test1') }}",
        "description": ""
      },
      {
        "sql": "IF OBJECT_ID (N'{{ id(destSchemaName) }}.{{ id('tmp_test2') }}', N'U') IS NOT NULL DROP TABLE {{ id(destSchemaName) }}.{{ id('tmp_test2') }}",
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
            JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT
        )['output'];

        $configuration->setConfiguration([
            'migrations' => [/** we don't care about migrations they can be empty */],
            'queriesOverride' => [
                'ingestionFullLoad' => $configOnError,
            ],
        ]);
        $this->componentsClient->updateConfiguration($configuration);

        try {
            $this->loadTable($tableId);
            $this->fail('import should fail');
        } catch (ClientException $e) {
            $this->assertStringStartsWith('Execution of custom table query failed because of', $e->getMessage());
        }

        /**
         * Test if tables are cleared
         */
        $configuration->setConfiguration([
            'migrations' => [/** we don't care about migrations they can be empty */],
            'queriesOverride' => [
                'ingestionFullLoad' => $testConfig,
            ],
        ]);
        $this->componentsClient->updateConfiguration($configuration);
        $this->loadTable($tableId); // now should succeed as tables were cleared in cleanup
    }

    public function testLoadIncrementalFromFileToTable(): void
    {
        $tableName = 'custom-table-2';

        $json = /** @lang JSON */<<<JSON
{
  "action": "generate",
  "backend": "synapse",
  "operation": "importIncremental",
  "source": "fileAbs",
  "columns": [
    "id",
    "NAME"
  ],
  "primaryKeys": [
    "id"
  ],
  "output": {
    "queries": [
      {
        "sql": "CREATE TABLE {{ id(destSchemaName) }}.{{ id(stageTableName) }} ([id] NVARCHAR(4000), [NAME] NVARCHAR(4000)) WITH (DISTRIBUTION = ROUND_ROBIN,CLUSTERED COLUMNSTORE INDEX)",
        "description": ""
      },
      {
        "sql": "COPY INTO {{ id(destSchemaName) }}.{{ id(stageTableName) }}\\nFROM {{ listFiles(sourceFiles) }}\\nWITH (\\n    FILE_TYPE='CSV',\\n    CREDENTIAL=(IDENTITY='Managed Identity'),\\n    FIELDQUOTE='\"',\\n    FIELDTERMINATOR=',',\\n    ENCODING = 'UTF8',\\n    \\n    IDENTITY_INSERT = 'OFF'\\n    ,FIRSTROW=2\\n)",
        "description": ""
      },
      {
        "sql": "CREATE TABLE {{ id(destSchemaName) }}.{{ id(destTableName ~ rand ~ '_tmp') }} ([id] NVARCHAR(4000), [NAME] NVARCHAR(4000)) WITH (DISTRIBUTION = ROUND_ROBIN,CLUSTERED COLUMNSTORE INDEX)",
        "description": ""
      },
      {
        "sql": "BEGIN TRANSACTION",
        "description": ""
      },
      {
        "sql": "UPDATE {{ id(destSchemaName) }}.{{ id(destTableName) }} SET [NAME] = COALESCE([src].[NAME], '') FROM {{ id(destSchemaName) }}.{{ id(stageTableName) }} AS [src] WHERE {{ id(destSchemaName) }}.{{ id(destTableName) }}.[id] = [src].[id] AND (COALESCE(CAST({{ id(destSchemaName) }}.{{ id(destTableName) }}.[NAME] AS NVARCHAR), '') != COALESCE([src].[NAME], '')) ",
        "description": ""
      },
      {
        "sql": "DELETE {{ id(destSchemaName) }}.{{ id(stageTableName) }} WHERE EXISTS (SELECT * FROM {{ id(destSchemaName) }}.{{ id(destTableName) }} WHERE {{ id(destSchemaName) }}.{{ id(destTableName) }}.[id] = {{ id(destSchemaName) }}.{{ id(stageTableName) }}.[id])",
        "description": ""
      },
      {
        "sql": "INSERT INTO {{ id(destSchemaName) }}.{{ id(destTableName ~ rand ~ '_tmp') }} ([id], [NAME]) SELECT a.[id],a.[NAME] FROM (SELECT [id], [NAME], ROW_NUMBER() OVER (PARTITION BY [id] ORDER BY [id]) AS \"_row_number_\" FROM {{ id(destSchemaName) }}.{{ id(stageTableName) }}) AS a WHERE a.\"_row_number_\" = 1",
        "description": ""
      },
      {
        "sql": "INSERT INTO {{ id(destSchemaName) }}.{{ id(destTableName) }} ([id], [NAME]) (SELECT CAST(COALESCE([id], '') as NVARCHAR) AS [id],CAST(COALESCE([NAME], '') as NVARCHAR) AS [NAME] FROM {{ id(destSchemaName) }}.{{ id(destTableName ~ rand ~ '_tmp') }} AS [src])",
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
        $jsonDecoded = json_decode(
            $json,
            true,
            512,
            JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT
        );

        $queriesOverride = [];
        $queriesOverride['ingestionIncrementalLoad'] = $jsonDecoded['output'];

        $tableId = $this->prepareTableWithConfiguration($tableName, [
                'migrations' => [
                    [
                        'sql' => /** @lang TSQL */ <<<SQL
                        CREATE TABLE {{ id(bucketName) }}.{{ id(tableName) }} ([id] INTEGER, [NAME] VARCHAR(100))
                        SQL,
                        'description' => 'first ever',
                    ],
                    [
                        'sql' => /** @lang TSQL */ <<<SQL
                        INSERT INTO {{ id(bucketName) }}.{{ id(tableName) }} ([id], [NAME]) SELECT 0, '- unchecked -';
                        INSERT INTO {{ id(bucketName) }}.{{ id(tableName) }} ([id], [NAME]) SELECT 26, 'czech';
                        INSERT INTO {{ id(bucketName) }}.{{ id(tableName) }} ([id], [NAME]) SELECT 1, 'english';
                        INSERT INTO {{ id(bucketName) }}.{{ id(tableName) }} ([id], [NAME]) SELECT 11, 'finnish';
                        INSERT INTO {{ id(bucketName) }}.{{ id(tableName) }} ([id], [NAME]) SELECT 24, 'french';
                        SQL,
                        'description' => 'initial data',
                    ],
                ],
                'queriesOverride' => $queriesOverride,
            ]
        );

        $csvFile = new CsvFile(__DIR__ . '/../../_data/languages.increment.csv');
        $fileId = $this->_client->uploadFile(
            $csvFile->getPathname(),
            (new FileUploadOptions())
                ->setNotify(false)
                ->setIsPublic(false)
                ->setCompress(true)
                ->setTags(['table-import'])
        );

        $this->_client->writeTableAsyncDirect($tableId, [
            'dataFileId' => $fileId,
            'incremental' => true,
        ]);

        $table = $this->_client->getTable($tableId);

        $this->assertEquals(['id', 'NAME'], $table['columns']);
        $this->assertSame(6, $table['rowsCount']);
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
        $events = $this->listEventsFilteredByName($this->client, 'storage.tableWithConfigurationImportQuery', $tableId, 50);
        $this->assertCount(9, $events);

        $events = $this->listEventsFilteredByName($this->client, 'storage.tableImportDone', $tableId, 10);
        $this->assertCount(1, $events);
    }

}
