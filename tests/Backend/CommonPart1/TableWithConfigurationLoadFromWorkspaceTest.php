<?php

namespace Keboola\Test\Backend\CommonPart1;

use Keboola\StorageApi\Client;
use Keboola\StorageApi\Components;
use Keboola\Test\Backend\TableWithConfigurationUtils;
use Keboola\Test\Backend\WorkspaceConnectionTrait;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;
use Keboola\Test\Backend\Workspaces\ParallelWorkspacesTestCase;
use Keboola\Test\ClientProvider\ClientProvider;

class TableWithConfigurationLoadFromWorkspaceTest extends ParallelWorkspacesTestCase
{
    use TableWithConfigurationUtils;
    use WorkspaceConnectionTrait;

    public const DEFAULT_CONFIGURATION_MIGRATIONS = [
        [
            'sql' => /** @lang TSQL */
                <<<SQL
                CREATE TABLE {{ id(bucketName) }}.{{ id(tableName) }} ([id] INTEGER, [NAME] VARCHAR(100))
                SQL,
            'description' => 'first ever',
        ],
    ];

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

    private function loadTableFromWorkspace(
        string $tableId,
        array $writeTableOptions = []
    ): void {
        // create workspace and source table in workspace
        $workspace = $this->initTestWorkspace();

        $sourceTableName = 'languages';
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        $backend->dropTableIfExists($sourceTableName);

        $connection = $workspace['connection'];

        $db = $this->getDbConnectionSynapse($connection);

        $quotedSourceTableId = $db->getDatabasePlatform()->quoteIdentifier(sprintf(
            '%s.%s',
            $connection['schema'],
            $sourceTableName,
        ));

        // data from `../../_data/languages.incremental.csv`
        $db->executeQuery("CREATE TABLE $quotedSourceTableId (
			[Id] INTEGER,
			[NAME] VARCHAR(100)
		);");
        $db->executeQuery("INSERT INTO $quotedSourceTableId ([id], [NAME]) SELECT 24, 'french'");
        $db->executeQuery("INSERT INTO $quotedSourceTableId ([id], [NAME]) SELECT 25, 'russian'");
        $db->executeQuery("INSERT INTO $quotedSourceTableId ([id], [NAME]) SELECT 26, 'slovak'");

        $this->_client->writeTableAsyncDirect($tableId, [
                'dataWorkspaceId' => $workspace['id'],
                'dataObject' => $sourceTableName,
            ] + $writeTableOptions);
    }

    public function testLoadFromWorkspaceToTable(): void
    {
        $tableName = 'custom-table-3';

        $json = /** @lang JSON */
            <<<JSON
{
  "action": "generate",
  "backend": "synapse",
  "operation": "importFull",
  "source": "table",
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
        "sql": "INSERT INTO {{ id(destSchemaName) }}.{{ id(stageTableName) }} ([id], [NAME]) SELECT [id], [NAME] FROM {{ id(sourceSchemaName) }}.{{ id(sourceTableName) }}",
        "description": ""
      },
      {
        "sql": "CREATE TABLE {{ id(destSchemaName) }}.{{ id(destTableName ~ rand ~ '_tmp') }} WITH (DISTRIBUTION=ROUND_ROBIN,CLUSTERED COLUMNSTORE INDEX) AS SELECT a.[id],a.[NAME] FROM (SELECT COALESCE([id], '') AS [id],COALESCE([NAME], '') AS [NAME], ROW_NUMBER() OVER (PARTITION BY [id] ORDER BY [id]) AS \"_row_number_\" FROM {{ id(destSchemaName) }}.{{ id(stageTableName) }}) AS a WHERE a.\"_row_number_\" = 1",
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

        [$tableId,] = $this->createTableWithConfiguration($json, $tableName, 'outputMappingFullLoad');

        $this->loadTableFromWorkspace(
            $tableId,
        );

        $table = $this->_client->getTable($tableId);

        $this->assertEquals(['id', 'NAME'], $table['columns']);
        $this->assertSame(3, $table['rowsCount']);
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
