<?php


namespace Keboola\Test\Backend\Workspaces;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Metadata;
use Keboola\Test\Backend\WorkspaceConnectionTrait;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;

class MetadataFromRedshiftWorkspaceTest extends ParallelWorkspacesTestCase
{
    use WorkspaceConnectionTrait;

    public function setUp()
    {
        parent::setUp();

        $token = $this->_client->verifyToken();

        if (!in_array('storage-types', $token['owner']['features'])) {
            $this->fail(sprintf('Metadata from workspaces are not enabled for project "%s"', $token['owner']['id']));
        }
    }

    public function testCreateTableFromWorkspace()
    {
        // create workspace and source table in workspace
        $workspace = $this->initTestWorkspace(self::BACKEND_REDSHIFT);

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        $backend->dropTableIfExists('test.metadata_columns');

        $db = $this->getDbConnection($workspace['connection']);
        $db->query("create table \"test.metadata_columns\" (
                    \"string\" varchar(16) not null default 'string',
                    \"char\" char null,
                    \"smallint\" smallint null,
                    \"integer\" integer not null default 4,
                    \"bigint\" bigint null,
                    \"decimal\" decimal(10,3) not null default 234.123,
                    \"real\" real null,
                    \"double\" double precision null,
                    \"boolean\" boolean not null default true,
                    \"date\" date not null default current_date,
                    \"timestamp\" timestamp not null default sysdate,
                    \"timestamptz\" timestamptz not null default sysdate 
                );");
        // create table from workspace
        $tableId = $this->_client->createTableAsyncDirect($this->getTestBucketId(self::STAGE_IN), array(
            'name' => 'metadata_columns',
            'dataWorkspaceId' => $workspace['id'],
            'dataTableName' => 'test.metadata_columns',
        ));
        $expectedStringMetadata = [
            'KBC.datatype.type' => 'VARCHAR',
            'KBC.datatype.nullable' => '',
            'KBC.datatype.basetype' => 'STRING',
            'KBC.datatype.length' => '16',
            'KBC.datatype.default' => 'string',
        ];
        $expectedCharMetadata = [
            'KBC.datatype.type' => 'BPCHAR',
            'KBC.datatype.nullable' => '1',
            'KBC.datatype.basetype' => 'STRING',
        ];
        $expectedSmallintMetadata = [
            'KBC.datatype.type' => 'INT2',
            'KBC.datatype.nullable' => '1',
            'KBC.datatype.basetype' => 'INTEGER',
        ];
        $expectedIntegerMetadata = [
            'KBC.datatype.type' => 'INT4',
            'KBC.datatype.nullable' => '',
            'KBC.datatype.basetype' => 'INTEGER',
            'KBC.datatype.default' => '4',
        ];
        $expectedBigIntMetadata = [
            'KBC.datatype.type' => 'INT8',
            'KBC.datatype.nullable' => '1',
            'KBC.datatype.basetype' => 'INTEGER',
        ];
        $expectedDecimalMetadata = [
            'KBC.datatype.type' => 'NUMERIC',
            'KBC.datatype.nullable' => '',
            'KBC.datatype.basetype' => 'NUMERIC',
            'KBC.datatype.length' => '10,3',
            'KBC.datatype.default' => '234.123',
        ];
        $expectedRealMetadata = [
            'KBC.datatype.type' => 'FLOAT4',
            'KBC.datatype.nullable' => '1',
            'KBC.datatype.basetype' => 'FLOAT',
        ];
        $expectedDoubleMetadata = [
            'KBC.datatype.type' => 'FLOAT8',
            'KBC.datatype.nullable' => '1',
            'KBC.datatype.basetype' => 'FLOAT',
        ];
        $expectedBooleanMetadata = [
            'KBC.datatype.type' => 'BOOL',
            'KBC.datatype.nullable' => '',
            'KBC.datatype.basetype' => 'BOOLEAN',
            'KBC.datatype.default' => 'true',
        ];
        $expectedDateMetadata = [
            'KBC.datatype.type' => 'DATE',
            'KBC.datatype.nullable' => '',
            'KBC.datatype.basetype' => 'DATE',
            'KBC.datatype.default' => '(\'now\'::text)::date',
        ];
        $expectedTimestampMetadata = [
            'KBC.datatype.type' => 'TIMESTAMP',
            'KBC.datatype.nullable' => '',
            'KBC.datatype.basetype' => 'TIMESTAMP',
            'KBC.datatype.default' => '(\'now\'::text)::timestamp without time zone',
        ];
        $expectedTimestamptzMetadata = [
            'KBC.datatype.type' => 'TIMESTAMPTZ',
            'KBC.datatype.nullable' => '',
            'KBC.datatype.basetype' => 'TIMESTAMP',
            'KBC.datatype.default' => '(\'now\'::text)::timestamp without time zone',
        ];
        // check that the new table has the correct metadata
        $table = $this->_client->getTable($tableId);

        $this->assertEquals([], $table['metadata']);
        $this->assertArrayHasKey('string', $table['columnMetadata']);
        $this->assertMetadata($expectedStringMetadata, $table['columnMetadata']['string']);
        $this->assertArrayHasKey('char', $table['columnMetadata']);
        $this->assertMetadata($expectedCharMetadata, $table['columnMetadata']['char']);
        $this->assertArrayHasKey('smallint', $table['columnMetadata']);
        $this->assertMetadata($expectedSmallintMetadata, $table['columnMetadata']['smallint']);
        $this->assertArrayHasKey('integer', $table['columnMetadata']);
        $this->assertMetadata($expectedIntegerMetadata, $table['columnMetadata']['integer']);
        $this->assertArrayHasKey('bigint', $table['columnMetadata']);
        $this->assertMetadata($expectedBigIntMetadata, $table['columnMetadata']['bigint']);
        $this->assertArrayHasKey('decimal', $table['columnMetadata']);
        $this->assertMetadata($expectedDecimalMetadata, $table['columnMetadata']['decimal']);
        $this->assertArrayHasKey('real', $table['columnMetadata']);
        $this->assertMetadata($expectedRealMetadata, $table['columnMetadata']['real']);
        $this->assertArrayHasKey('double', $table['columnMetadata']);
        $this->assertMetadata($expectedDoubleMetadata, $table['columnMetadata']['double']);
        $this->assertArrayHasKey('boolean', $table['columnMetadata']);
        $this->assertMetadata($expectedBooleanMetadata, $table['columnMetadata']['boolean']);
        $this->assertArrayHasKey('date', $table['columnMetadata']);
        $this->assertMetadata($expectedDateMetadata, $table['columnMetadata']['date']);
        $this->assertArrayHasKey('timestamp', $table['columnMetadata']);
        $this->assertMetadata($expectedTimestampMetadata, $table['columnMetadata']['timestamp']);
        $this->assertArrayHasKey('timestamptz', $table['columnMetadata']);
        $this->assertMetadata($expectedTimestamptzMetadata, $table['columnMetadata']['timestamptz']);
    }

    public function testCopyImport()
    {
        $table_id = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'languages3',
            new CsvFile(__DIR__ . '/../../_data/languages.csv'),
            array('primaryKey' => 'id')
        );

        // create workspace and source table in workspace
        $workspace = $this->initTestWorkspace(self::BACKEND_REDSHIFT);

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        $backend->dropTableIfExists('test.Languages3');

        $db = $this->getDbConnection($workspace['connection']);
        $db->query("create table \"test.Languages3\" (
                \"id\" integer not null,
                \"name\" varchar not null default 'honza'
            );");
        $db->query("insert into \"test.Languages3\" (\"id\", \"name\") values (1, 'cz'), (2, 'en');");
        $this->_client->writeTableAsyncDirect($table_id, array(
            'dataWorkspaceId' => $workspace['id'],
            'dataTableName' => 'test.Languages3',
        ));
        $expected = array(
            '"id","name"',
            '"1","cz"',
            '"2","en"',
        );
        $this->assertLinesEqualsSorted(
            implode("\n", $expected) . "\n",
            $this->_client->getTableDataPreview($table_id, array('format' => 'rfc')),
            'imported data comparsion'
        );

        // check that the new table has the correct metadata
        $table = $this->_client->getTable($table_id);

        $this->assertEquals([], $table['metadata']);
        $this->assertEquals([], $table['columnMetadata']);
        $db->query("truncate table \"test.Languages3\"");
        $db->query("alter table \"test.Languages3\" ADD COLUMN \"update\" varchar(64)");
        $db->query("insert into \"test.Languages3\" values " .
            "(1, 'cz', null)," .
            " (3, 'sk', 'newValue')," .
            " (4, 'jp', 'test');");
        $this->_client->writeTableAsyncDirect($table['id'], array(
            'dataWorkspaceId' => $workspace['id'],
            'dataTableName' => 'test.Languages3',
            'incremental' => true,
        ));
        $expected = array(
            '"id","name","update"',
            '"1","cz",""',
            '"2","en",""',
            '"3","sk","newValue"',
            '"4","jp","test"',
        );
        $this->assertLinesEqualsSorted(
            implode("\n", $expected) . "\n",
            $this->_client->getTableDataPreview($table['id'], array('format' => 'rfc')),
            'new  column added'
        );
        $table = $this->_client->getTable($table['id']);
        $this->assertEquals([], $table['metadata']);
        $this->assertEquals([], $table['columnMetadata']);
    }

    public function testWriteTableFromWorkspaceWithUnsupportedDataType()
    {
        $tableId = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'metadata_columns',
            new CsvFile(__DIR__ . '/../../_data/languages.csv')
        );

        // create workspace and source table in workspace
        $workspace = $this->initTestWorkspace(self::BACKEND_REDSHIFT);

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        $backend->dropTableIfExists('test.metadata_columns');

        $db = $this->getDbConnection($workspace['connection']);

         $db->query("create table \"test.metadata_columns\" (
                \"id\" integer not null,
                \"name\" geometry
            );");

        try {
            $this->_client->writeTableAsyncDirect($tableId, array(
            'dataWorkspaceId' => $workspace['id'],
            'dataTableName' => 'test.metadata_columns',
            ));
            $this->fail('Exception "cannot cast type geometry to character " should be thrown');
        } catch (ClientException $e) {
            $this->assertSame(
                "SQLSTATE[42846]: Cannot coerce: 7 ERROR:  cannot cast type geometry to character varying",
                $e->getMessage()
            );
        }

        $table = $this->_client->getTable($tableId);

        $this->assertEquals(2, count($table['columns']));
        $this->assertEquals([], $table['columnMetadata']);
    }

    public function testCreateTableFromWorkspaceWithUnsupportedDataType()
    {
        // create workspace and source table in workspace
        $workspace = $this->initTestWorkspace(self::BACKEND_REDSHIFT);

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        $backend->dropTableIfExists('test.metadata_columns');

        $db = $this->getDbConnection($workspace['connection']);

        $db->query("create table \"test.metadata_columns\" (
                \"id\" integer not null,
                \"name\" geometry
            );");

        try {
            $tableId = $this->_client->createTableAsyncDirect($this->getTestBucketId(self::STAGE_IN), array(
                'name' => 'metadata_columns',
                'dataWorkspaceId' => $workspace['id'],
                'dataTableName' => 'test.metadata_columns',
            ));

            $table = $this->_client->getTable($tableId);

            $this->assertEquals(2, count($table['columns']));
            $this->assertEquals([], $table['columnMetadata']);
            $this->fail('Exception "cannot cast type geometry to character " should be thrown');
        } catch (ClientException $e) {
            $this->assertSame(
                "SQLSTATE[42846]: Cannot coerce: 7 ERROR:  cannot cast type geometry to character varying",
                $e->getMessage()
            );
        }
    }

    private function assertMetadata($expectedKeyValues, $metadata)
    {
        $this->assertEquals(count($expectedKeyValues), count($metadata));
        foreach ($metadata as $data) {
            $this->assertArrayHasKey("key", $data);
            $this->assertArrayHasKey("value", $data);
            $this->assertEquals($expectedKeyValues[$data['key']], $data['value']);
            $this->assertArrayHasKey("provider", $data);
            $this->assertArrayHasKey("timestamp", $data);
            $this->assertRegExp(self::ISO8601_REGEXP, $data['timestamp']);
            $this->assertEquals(Metadata::PROVIDER_STORAGE, $data['provider']);
        }
    }
}
