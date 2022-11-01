<?php

namespace Keboola\Test\Backend\CommonPart1;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Components;
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

        $json = /** @lang JSON */<<<JSON
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
        $jsonDecoded = json_decode(
            $json,
            true,
            512,
            JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT
        );

        $queriesOverride = [];
        $queriesOverride['ingestionFullLoad'] = $jsonDecoded['output'];

        // HTML NOWDOC used so that autoformat does not reformat SQL queries inside the strings
        $tableId = $this->prepareTableWithConfiguration($tableName, [
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
}
