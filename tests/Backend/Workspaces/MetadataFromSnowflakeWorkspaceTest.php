<?php


namespace Keboola\Test\Backend\Workspaces;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Metadata;
use Keboola\Test\Backend\WorkspaceConnectionTrait;
use Keboola\Test\Utils\EventsQueryBuilder;

class MetadataFromSnowflakeWorkspaceTest extends ParallelWorkspacesTestCase
{
    use WorkspaceConnectionTrait;

    public function setUp(): void
    {
        parent::setUp();

        $token = $this->_client->verifyToken();

        if (!in_array('storage-types', $token['owner']['features'])) {
            $this->fail(sprintf('Metadata from workspaces are not enabled for project "%s"', $token['owner']['id']));
        }
    }

    public function testIncrementalLoadUpdateDataType(): void
    {
        $this->initEvents($this->_client);
        // create workspace and source table in workspace
        $workspace = $this->initTestWorkspace(self::BACKEND_SNOWFLAKE);

        $db = $this->getDbConnection($workspace['connection']);
        $db->query('create or replace table "test.metadata_columns" (
                    "id" varchar(16),
                    "name" varchar
                );');

        $tableId = $this->_client->createTableAsyncDirect($this->getTestBucketId(self::STAGE_IN), [
            'name' => 'metadata_columns',
            'dataWorkspaceId' => $workspace['id'],
            'dataTableName' => 'test.metadata_columns',
        ]);

        $expectedNameMetadata = [
            'KBC.datatype.type' => 'VARCHAR',
            'KBC.datatype.nullable' => '1',
            'KBC.datatype.basetype' => 'STRING',
            'KBC.datatype.length' => '16777216',
        ];

        $expectedIdMetadata = [
            'KBC.datatype.type' => 'VARCHAR',
            'KBC.datatype.nullable' => '1',
            'KBC.datatype.basetype' => 'STRING',
            'KBC.datatype.length' => '16',
        ];

        $table = $this->_client->getTable($tableId);

        $this->assertEquals([], $table['metadata']);

        $this->assertArrayHasKey('id', $table['columnMetadata']);
        $this->assertMetadata($expectedIdMetadata, $table['columnMetadata']['id']);
        $this->assertArrayHasKey('name', $table['columnMetadata']);
        $this->assertMetadata($expectedNameMetadata, $table['columnMetadata']['name']);

        $db->query('create or replace table "test.metadata_columns" (
                    "id" integer,
                    "name" char not null
                );');

        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);

        // incremental load will not update datatype basetype as basetype in workspace is different than in table
        $this->_client->writeTableAsyncDirect($tableId, [
            'incremental' => true,
            'dataWorkspaceId' => $workspace['id'],
            'dataTableName' => 'test.metadata_columns',
        ]);

        $assertCallback = function ($events) use ($tableId) {
            $this->assertCount(1, $events, $tableId);
            // type event assert
            $notUpdateColumnTypeEvent = $events[0];
            $this->assertSame('storage.tableAutomaticDataTypesNotUpdateColumnType', $notUpdateColumnTypeEvent['event']);
            $this->assertSame('storage', $notUpdateColumnTypeEvent['component']);
            $this->assertSame('warn', $notUpdateColumnTypeEvent['type']);
            $this->assertArrayHasKey('params', $notUpdateColumnTypeEvent);
            $this->assertSame($tableId, $notUpdateColumnTypeEvent['objectId']);
            $this->assertSame('id', $notUpdateColumnTypeEvent['params']['column']);
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.tableAutomaticDataTypesNotUpdateColumnType')
            ->setTokenId($this->tokenId)
            ->setObjectId($tableId);
        $this->assertEventWithRetries($this->_client, $assertCallback, $query);

        $assertCallback = function ($events) use ($tableId) {
            $this->assertCount(1, $events, $tableId);
            // nullable event assert
            $notUpdateNullableColumnEvent = $events[0];
            $this->assertSame('storage.tableAutomaticDataTypesNotUpdateColumnNullable', $notUpdateNullableColumnEvent['event']);
            $this->assertSame('storage', $notUpdateNullableColumnEvent['component']);
            $this->assertSame('warn', $notUpdateNullableColumnEvent['type']);
            $this->assertArrayHasKey('params', $notUpdateNullableColumnEvent);
            $this->assertSame($tableId, $notUpdateNullableColumnEvent['objectId']);
            $this->assertSame('name', $notUpdateNullableColumnEvent['params']['column']);
        };

        $query = new EventsQueryBuilder();
        $query->setEvent('storage.tableAutomaticDataTypesNotUpdateColumnNullable')
            ->setTokenId($this->tokenId)
            ->setObjectId($tableId);
        $this->assertEventWithRetries($this->_client, $assertCallback, $query);

        $assertCallback = function ($events) use ($tableId) {
            $this->assertCount(1, $events, $tableId);
            // length event assert
            $notUpdateLengthEvent = $events[0];
            $this->assertSame('storage.tableAutomaticDataTypesNotUpdateColumnLength', $notUpdateLengthEvent['event']);
            $this->assertSame('storage', $notUpdateLengthEvent['component']);
            $this->assertSame('warn', $notUpdateLengthEvent['type']);
            $this->assertArrayHasKey('params', $notUpdateLengthEvent);
            $this->assertSame($tableId, $notUpdateLengthEvent['objectId']);
            $this->assertSame('name', $notUpdateLengthEvent['params']['column']);
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.tableAutomaticDataTypesNotUpdateColumnLength')
            ->setTokenId($this->tokenId)
            ->setObjectId($tableId);
        $this->assertEventWithRetries($this->_client, $assertCallback, $query);

        $table = $this->_client->getTable($tableId);

        $this->assertEquals([], $table['metadata']);

        $this->assertArrayHasKey('id', $table['columnMetadata']);
        $this->assertMetadata($expectedIdMetadata, $table['columnMetadata']['id']);
        $this->assertArrayHasKey('name', $table['columnMetadata']);
        $this->assertMetadata($expectedNameMetadata, $table['columnMetadata']['name']);

        //only full load will update datatype length
        $this->_client->writeTableAsyncDirect($tableId, [
            'dataWorkspaceId' => $workspace['id'],
            'dataTableName' => 'test.metadata_columns',
        ]);

        $expectedNameMetadata = [
            'KBC.datatype.type' => 'VARCHAR',
            'KBC.datatype.nullable' => '',
            'KBC.datatype.basetype' => 'STRING',
            'KBC.datatype.length' => '1',
        ];

        $expectedIdMetadata = [
            'KBC.datatype.type' => 'NUMBER',
            'KBC.datatype.nullable' => '1',
            'KBC.datatype.basetype' => 'INTEGER',
            'KBC.datatype.length' => '38,0',
        ];

        $table = $this->_client->getTable($tableId);

        $this->assertEquals([], $table['metadata']);

        $this->assertArrayHasKey('id', $table['columnMetadata']);
        $this->assertMetadata($expectedIdMetadata, $table['columnMetadata']['id']);
        $this->assertArrayHasKey('name', $table['columnMetadata']);
        $this->assertMetadata($expectedNameMetadata, $table['columnMetadata']['name']);

        $db->query('create or replace table "test.metadata_columns" (
                    "id" integer,
                    "name" varchar(32)
                );');

        $this->_client->writeTableAsyncDirect($tableId, [
            'incremental' => true,
            'dataWorkspaceId' => $workspace['id'],
            'dataTableName' => 'test.metadata_columns',
        ]);

        $expectedNameMetadata = [
            'KBC.datatype.type' => 'VARCHAR',
            'KBC.datatype.nullable' => '1',
            'KBC.datatype.basetype' => 'STRING',
            'KBC.datatype.length' => '32',
        ];

        $expectedIdMetadata = [
            'KBC.datatype.type' => 'NUMBER',
            'KBC.datatype.nullable' => '1',
            'KBC.datatype.basetype' => 'INTEGER',
            'KBC.datatype.length' => '38,0',
        ];

        $table = $this->_client->getTable($tableId);

        $this->assertEquals([], $table['metadata']);

        $this->assertArrayHasKey('id', $table['columnMetadata']);
        $this->assertMetadata($expectedIdMetadata, $table['columnMetadata']['id']);
        $this->assertArrayHasKey('name', $table['columnMetadata']);
        $this->assertMetadata($expectedNameMetadata, $table['columnMetadata']['name']);
    }

    public function testCreateTableFromWorkspace(): void
    {
        // create workspace and source table in workspace
        $workspace = $this->initTestWorkspace(self::BACKEND_SNOWFLAKE);

        $db = $this->getDbConnection($workspace['connection']);
        $db->query("create or replace table \"test.metadata_columns\" (
                    \"string\" varchar(16) not null default 'string',
                    \"char\" char null,
                    \"integer\" integer not null default 4,
                    \"decimal\" decimal(10,3) not null default 234.123,
                    \"real\" real null,
                    \"double\" double precision null,
                    \"boolean\" boolean not null default true,
                    \"variant\" variant,
                    \"time\" time not null default current_time,
                    \"date\" date not null default current_date,
                    \"timestamp\" timestamp not null default current_timestamp,
                    \"timestampltz\" timestampltz not null default current_timestamp 
                );");
        // create table from workspace
        $tableId = $this->_client->createTableAsyncDirect($this->getTestBucketId(self::STAGE_IN), [
            'name' => 'metadata_columns',
            'dataWorkspaceId' => $workspace['id'],
            'dataTableName' => 'test.metadata_columns',
        ]);
        $expectedStringMetadata = [
            'KBC.datatype.type' => 'VARCHAR',
            'KBC.datatype.nullable' => '',
            'KBC.datatype.basetype' => 'STRING',
            'KBC.datatype.length' => '16',
            'KBC.datatype.default' => '\'string\'',
        ];
        $expectedCharMetadata = [
            'KBC.datatype.type' => 'VARCHAR',
            'KBC.datatype.nullable' => '1',
            'KBC.datatype.basetype' => 'STRING',
            'KBC.datatype.length' => '1',
        ];
        $expectedIntegerMetadata = [
            'KBC.datatype.type' => 'NUMBER',
            'KBC.datatype.nullable' => '',
            'KBC.datatype.basetype' => 'INTEGER',
            'KBC.datatype.length' => '38,0',
            'KBC.datatype.default' => '4',
        ];
        $expectedDecimalMetadata = [
            'KBC.datatype.type' => 'NUMBER',
            'KBC.datatype.nullable' => '',
            'KBC.datatype.basetype' => 'NUMERIC',
            'KBC.datatype.length' => '10,3',
            'KBC.datatype.default' => '234.123',
        ];
        $expectedRealMetadata = [
            'KBC.datatype.type' => 'FLOAT',
            'KBC.datatype.nullable' => '1',
            'KBC.datatype.basetype' => 'FLOAT',
        ];
        $expectedDoubleMetadata = [
            'KBC.datatype.type' => 'FLOAT',
            'KBC.datatype.nullable' => '1',
            'KBC.datatype.basetype' => 'FLOAT',
        ];
        $expectedBooleanMetadata = [
            'KBC.datatype.type' => 'BOOLEAN',
            'KBC.datatype.nullable' => '',
            'KBC.datatype.basetype' => 'BOOLEAN',
            'KBC.datatype.default' => 'TRUE',
        ];
        $expectedVariantMetadata = [
            'KBC.datatype.type' => 'VARIANT',
            'KBC.datatype.nullable' => '1',
            'KBC.datatype.basetype' => 'STRING',
        ];
        $expectedTimeMetadata = [
            'KBC.datatype.type' => 'TIME',
            'KBC.datatype.nullable' => '',
            'KBC.datatype.basetype' => 'STRING',
            'KBC.datatype.default' => 'CURRENT_TIME()',
            'KBC.datatype.length' => '9',
        ];
        $expectedDateMetadata = [
            'KBC.datatype.type' => 'DATE',
            'KBC.datatype.nullable' => '',
            'KBC.datatype.basetype' => 'DATE',
            'KBC.datatype.default' => 'CURRENT_DATE()',
        ];
        $expectedTimestampMetadata = [
            'KBC.datatype.type' => 'TIMESTAMP_NTZ',
            'KBC.datatype.nullable' => '',
            'KBC.datatype.basetype' => 'TIMESTAMP',
            'KBC.datatype.default' => 'CURRENT_TIMESTAMP()',
            'KBC.datatype.length' => '9',
            ];
        $expectedTimestamptzMetadata = [
            'KBC.datatype.type' => 'TIMESTAMP_LTZ',
            'KBC.datatype.nullable' => '',
            'KBC.datatype.basetype' => 'TIMESTAMP',
            'KBC.datatype.default' => 'CURRENT_TIMESTAMP()',
            'KBC.datatype.length' => '9',
        ];
        // check that the new table has the correct metadata
        $table = $this->_client->getTable($tableId);

        $this->assertEquals([], $table['metadata']);
        $this->assertArrayHasKey('string', $table['columnMetadata']);
        $this->assertMetadata($expectedStringMetadata, $table['columnMetadata']['string']);
        $this->assertArrayHasKey('char', $table['columnMetadata']);
        $this->assertMetadata($expectedCharMetadata, $table['columnMetadata']['char']);
        $this->assertArrayHasKey('integer', $table['columnMetadata']);
        $this->assertMetadata($expectedIntegerMetadata, $table['columnMetadata']['integer']);
        $this->assertArrayHasKey('decimal', $table['columnMetadata']);
        $this->assertMetadata($expectedDecimalMetadata, $table['columnMetadata']['decimal']);
        $this->assertArrayHasKey('real', $table['columnMetadata']);
        $this->assertMetadata($expectedRealMetadata, $table['columnMetadata']['real']);
        $this->assertArrayHasKey('double', $table['columnMetadata']);
        $this->assertMetadata($expectedDoubleMetadata, $table['columnMetadata']['double']);
        $this->assertArrayHasKey('boolean', $table['columnMetadata']);
        $this->assertMetadata($expectedBooleanMetadata, $table['columnMetadata']['boolean']);
        $this->assertArrayHasKey('variant', $table['columnMetadata']);
        $this->assertMetadata($expectedVariantMetadata, $table['columnMetadata']['variant']);
        $this->assertArrayHasKey('time', $table['columnMetadata']);
        $this->assertMetadata($expectedTimeMetadata, $table['columnMetadata']['time']);
        $this->assertArrayHasKey('date', $table['columnMetadata']);
        $this->assertMetadata($expectedDateMetadata, $table['columnMetadata']['date']);
        $this->assertArrayHasKey('timestamp', $table['columnMetadata']);
        $this->assertMetadata($expectedTimestampMetadata, $table['columnMetadata']['timestamp']);
        $this->assertArrayHasKey('timestampltz', $table['columnMetadata']);
        $this->assertMetadata($expectedTimestamptzMetadata, $table['columnMetadata']['timestampltz']);
    }

    public function testCopyImport(): void
    {
        $table_id = $this->_client->createTableAsync(
            $this->getTestBucketId(self::STAGE_IN),
            'languages3',
            new CsvFile(__DIR__ . '/../../_data/languages.csv'),
            ['primaryKey' => 'id'],
        );

        // create workspace and source table in workspace
        $workspace = $this->initTestWorkspace(self::BACKEND_SNOWFLAKE);

        $db = $this->getDbConnection($workspace['connection']);
        $db->query("create or replace table \"test.Languages3\" (
                \"id\" integer not null,
                \"name\" varchar not null default 'honza'
            );");
        $db->query("insert into \"test.Languages3\" (\"id\", \"name\") values (1, 'cz'), (2, 'en');");
        $this->_client->writeTableAsyncDirect($table_id, [
            'dataWorkspaceId' => $workspace['id'],
            'dataTableName' => 'test.Languages3',
        ]);
        $expected = [
            '"id","name"',
            '"1","cz"',
            '"2","en"',
        ];
        $this->assertLinesEqualsSorted(
            implode("\n", $expected) . "\n",
            $this->_client->getTableDataPreview($table_id, ['format' => 'rfc']),
            'imported data comparsion',
        );
        // check the created metadata
        $expectedIdMetadata = [
            'KBC.datatype.type' => 'NUMBER',
            'KBC.datatype.nullable' => '',
            'KBC.datatype.basetype' => 'INTEGER',
            'KBC.datatype.length' => '38,0',
        ];
        $expectedNameMetadata = [
            'KBC.datatype.type' => 'VARCHAR',
            'KBC.datatype.nullable' => '',
            'KBC.datatype.basetype' => 'STRING',
            'KBC.datatype.length' => '16777216',
            'KBC.datatype.default' => '\'honza\'',
        ];

        // check that the new table has the correct metadata
        $table = $this->_client->getTable($table_id);
        $this->assertEquals([], $table['metadata']);
        $this->assertArrayHasKey('id', $table['columnMetadata']);
        $this->assertMetadata($expectedIdMetadata, $table['columnMetadata']['id']);
        $this->assertArrayHasKey('name', $table['columnMetadata']);
        $this->assertMetadata($expectedNameMetadata, $table['columnMetadata']['name']);

        $db->query('truncate table "test.Languages3"');
        $db->query("alter table \"test.Languages3\" ADD COLUMN \"update\" varchar(64) NOT NULL DEFAULT '';");
        $db->query('insert into "test.Languages3" values ' .
            "(1, 'cz', '')," .
            " (3, 'sk', 'newValue')," .
            " (4, 'jp', 'test');");
        $this->_client->writeTableAsyncDirect($table['id'], [
            'dataWorkspaceId' => $workspace['id'],
            'dataTableName' => 'test.Languages3',
            'incremental' => true,
        ]);
        $expected = [
            '"id","name","update"',
            '"1","cz",""',
            '"2","en",""',
            '"3","sk","newValue"',
            '"4","jp","test"',
        ];

        $this->assertLinesEqualsSorted(
            implode("\n", $expected) . "\n",
            $this->_client->getTableDataPreview($table['id'], ['format' => 'rfc']),
            'new  column added',
        );
        $expectedUpdateMetadata = [
            'KBC.datatype.type' => 'VARCHAR',
            'KBC.datatype.nullable' => '',
            'KBC.datatype.basetype' => 'STRING',
            'KBC.datatype.length' => '64',
            'KBC.datatype.default' => '\'\'',
        ];
        $table = $this->_client->getTable($table['id']);
        $this->assertEquals([], $table['metadata']);
        $this->assertArrayHasKey('id', $table['columnMetadata']);
        $this->assertArrayHasKey('name', $table['columnMetadata']);
        $this->assertArrayHasKey('update', $table['columnMetadata']);
        $this->assertMetadata($expectedUpdateMetadata, $table['columnMetadata']['update']);
    }

    public function testWriteTableFromWorkspaceWithSnowflakeBug(): void
    {
        $tableId = $this->_client->createTableAsync(
            $this->getTestBucketId(self::STAGE_IN),
            'metadata_columns',
            new CsvFile(__DIR__ . '/../../_data/metadataBug.csv'),
        );

        // create workspace and source table in workspace
        $workspace = $this->initTestWorkspace(self::BACKEND_SNOWFLAKE);

        $db = $this->getDbConnection($workspace['connection']);
        $db->query("CREATE OR REPLACE TABLE \"test.metadata_columns\" AS SELECT
                        '1'::integer AS \"id\",
                        'roman'::string AS \"name\",
                        'test'::variant AS \"variant\",
                        PARSE_JSON('{\"id\":\"test\"}'):id as \"variant2\",
                        PARSE_JSON('{\"id\":\"test\"}'):id::variant as \"variant3\"
                    ;");

        $this->_client->writeTableAsyncDirect($tableId, [
            'dataWorkspaceId' => $workspace['id'],
            'dataTableName' => 'test.metadata_columns',
        ]);

        $table = $this->_client->getTable($tableId);

        $this->assertEquals(5, count($table['columns']));
        $this->assertMetadata([
            'KBC.datatype.type' => 'NUMBER',
            'KBC.datatype.nullable' => '1',
            'KBC.datatype.basetype' => 'INTEGER',
            'KBC.datatype.length' => '38,0',
        ], $table['columnMetadata']['id']);
        $this->assertMetadata([
            'KBC.datatype.type' => 'VARCHAR',
            'KBC.datatype.nullable' => '1',
            'KBC.datatype.basetype' => 'STRING',
            'KBC.datatype.length' => '16777216',
        ], $table['columnMetadata']['name']);
        $this->assertMetadata([
            'KBC.datatype.type' => 'VARIANT',
            'KBC.datatype.nullable' => '1',
            'KBC.datatype.basetype' => 'STRING',
        ], $table['columnMetadata']['variant']);
        $this->assertMetadata([
            'KBC.datatype.type' => 'VARIANT',
            'KBC.datatype.nullable' => '1',
            'KBC.datatype.basetype' => 'STRING',
        ], $table['columnMetadata']['variant2']);
        $this->assertMetadata([
            'KBC.datatype.type' => 'VARIANT',
            'KBC.datatype.nullable' => '1',
            'KBC.datatype.basetype' => 'STRING',
        ], $table['columnMetadata']['variant3']);
    }

    public function testCreateTableFromWorkspaceWithSnowflakeBug(): void
    {
        // create workspace and source table in workspace
        $workspace = $this->initTestWorkspace(self::BACKEND_SNOWFLAKE);

        $db = $this->getDbConnection($workspace['connection']);
        $db->query("CREATE OR REPLACE TABLE \"test.metadata_columns\" AS SELECT
                        '1'::integer AS \"id\",
                        'roman'::string AS \"name\",
                        'test'::variant AS \"variant\",
                        PARSE_JSON('{\"id\":\"test\"}'):id as \"variant2\",
                        PARSE_JSON('{\"id\":\"test\"}'):id::variant as \"variant3\"
                    ;");

        $tableId = $this->_client->createTableAsyncDirect($this->getTestBucketId(self::STAGE_IN), [
            'name' => 'metadata_columns',
            'dataWorkspaceId' => $workspace['id'],
            'dataTableName' => 'test.metadata_columns',
        ]);

        $table = $this->_client->getTable($tableId);

        $this->assertEquals(5, count($table['columns']));
        $this->assertMetadata([
            'KBC.datatype.type' => 'NUMBER',
            'KBC.datatype.nullable' => '1',
            'KBC.datatype.basetype' => 'INTEGER',
            'KBC.datatype.length' => '38,0',
        ], $table['columnMetadata']['id']);
        $this->assertMetadata([
            'KBC.datatype.type' => 'VARCHAR',
            'KBC.datatype.nullable' => '1',
            'KBC.datatype.basetype' => 'STRING',
            'KBC.datatype.length' => '16777216',
        ], $table['columnMetadata']['name']);
        $this->assertMetadata([
            'KBC.datatype.type' => 'VARIANT',
            'KBC.datatype.nullable' => '1',
            'KBC.datatype.basetype' => 'STRING',
        ], $table['columnMetadata']['variant']);
        $this->assertMetadata([
            'KBC.datatype.type' => 'VARIANT',
            'KBC.datatype.nullable' => '1',
            'KBC.datatype.basetype' => 'STRING',
        ], $table['columnMetadata']['variant2']);
        $this->assertMetadata([
            'KBC.datatype.type' => 'VARIANT',
            'KBC.datatype.nullable' => '1',
            'KBC.datatype.basetype' => 'STRING',
        ], $table['columnMetadata']['variant3']);
    }

    private function assertMetadata($expectedKeyValues, $metadata)
    {
        $this->assertEquals(count($expectedKeyValues), count($metadata));
        foreach ($metadata as $data) {
            $this->assertArrayHasKey('key', $data);
            $this->assertArrayHasKey('value', $data);
            $this->assertEquals($expectedKeyValues[$data['key']], $data['value']);
            $this->assertArrayHasKey('provider', $data);
            $this->assertArrayHasKey('timestamp', $data);
            $this->assertMatchesRegularExpression(self::ISO8601_REGEXP, $data['timestamp']);
            $this->assertEquals(Metadata::PROVIDER_STORAGE, $data['provider']);
        }
    }
}
