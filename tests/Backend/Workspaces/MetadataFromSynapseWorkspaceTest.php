<?php

namespace Keboola\Test\Backend\Workspaces;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Metadata;
use Keboola\Test\Backend\WorkspaceConnectionTrait;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;

class MetadataFromSynapseWorkspaceTest extends ParallelWorkspacesTestCase
{
    use WorkspaceConnectionTrait;

    public function setUp()
    {
        parent::setUp();

        $token = $this->_client->verifyToken();

        if (!in_array('storage-types', $token['owner']['features'])) {
            $this->fail(sprintf('Metadata from workspaces are not enabled for project "%s"', $token['owner']['id']));
        }

        if (!in_array('tables-definition', $token['owner']['features'])) {
            $this->fail(sprintf('Table definitions are not enabled for project "%s"', $token['owner']['id']));
        }
    }

    public function testCreateTableFromWorkspace()
    {
        // create workspace and source table in workspace
        $workspace = $this->initTestWorkspace(self::BACKEND_SYNAPSE);

        $tableId = 'metadata_columns';

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        $backend->dropTableIfExists($tableId);

        $connection = $workspace['connection'];
        $db = $this->getDbConnection($connection);

        $quotedTableId = $db->getDatabasePlatform()->quoteIdentifier(sprintf(
            '%s.%s',
            $connection['schema'],
            $tableId
        ));

        $db->query("create table $quotedTableId (
                    \"string\" varchar(16) not null default 'string',
                    \"char\" char null,
                    \"integer\" integer not null default 4,
                    \"decimal\" decimal(10,3) not null default 234.123,
                    \"real\" real default null,
                    \"double\" double precision default null,
                    \"boolean\" bit not null default 1,
                    \"time\" time not null,
                    \"date\" date not null
                );");
        // create table from workspace
        $tableId = $this->_client->createTableAsyncDirect($this->getTestBucketId(self::STAGE_IN), array(
            'name' => 'metadata_columns',
            'dataWorkspaceId' => $workspace['id'],
            'dataTableName' => $tableId,
        ));
        $expectedStringMetadata = [
            'KBC.datatype.type' => 'VARCHAR',
            'KBC.datatype.nullable' => '',
            'KBC.datatype.basetype' => 'STRING',
            'KBC.datatype.length' => '16',
            'KBC.datatype.default' => '\'string\'',
        ];
        $expectedCharMetadata = [
            'KBC.datatype.type' => 'CHAR',
            'KBC.datatype.nullable' => '1',
            'KBC.datatype.basetype' => 'STRING',
            'KBC.datatype.length' => '1',
        ];
        $expectedIntegerMetadata = [
            'KBC.datatype.type' => 'INT',
            'KBC.datatype.nullable' => '',
            'KBC.datatype.basetype' => 'INTEGER',
            'KBC.datatype.default' => '4',
        ];
        $expectedDecimalMetadata = [
            'KBC.datatype.type' => 'DECIMAL',
            'KBC.datatype.nullable' => '',
            'KBC.datatype.basetype' => 'NUMERIC',
            'KBC.datatype.length' => '10,3',
            'KBC.datatype.default' => '234.123',
        ];
        $expectedRealMetadata = [
            'KBC.datatype.type' => 'REAL',
            'KBC.datatype.nullable' => '1',
            'KBC.datatype.basetype' => 'FLOAT',
            'KBC.datatype.default' => 'NULL',
        ];
        $expectedDoubleMetadata = [
            'KBC.datatype.type' => 'FLOAT',
            'KBC.datatype.nullable' => '1',
            'KBC.datatype.basetype' => 'FLOAT',
            'KBC.datatype.length' => '53',
            'KBC.datatype.default' => 'NULL',
        ];
        $expectedBooleanMetadata = [
            'KBC.datatype.type' => 'BIT',
            'KBC.datatype.nullable' => '',
            'KBC.datatype.basetype' => 'BOOLEAN',
            'KBC.datatype.default' => '1',
        ];
        $expectedTimeMetadata = [
            'KBC.datatype.type' => 'TIME',
            'KBC.datatype.nullable' => '',
            'KBC.datatype.basetype' => 'TIMESTAMP',
            'KBC.datatype.length' => '7',
        ];
        $expectedDateMetadata = [
            'KBC.datatype.type' => 'DATE',
            'KBC.datatype.nullable' => '',
            'KBC.datatype.basetype' => 'DATE',
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
        $this->assertArrayHasKey('time', $table['columnMetadata']);
        $this->assertMetadata($expectedTimeMetadata, $table['columnMetadata']['time']);
        $this->assertArrayHasKey('date', $table['columnMetadata']);
        $this->assertMetadata($expectedDateMetadata, $table['columnMetadata']['date']);
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
        $workspace = $this->initTestWorkspace(self::BACKEND_SYNAPSE);

        $tableId = 'Languages3';

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        $backend->dropTableIfExists($tableId);

        $connection = $workspace['connection'];
        $db = $this->getDbConnection($connection);

        $quotedTableId = $db->getDatabasePlatform()->quoteIdentifier(sprintf(
            '%s.%s',
            $connection['schema'],
            $tableId
        ));

        $db->query("create table $quotedTableId (
                \"id\" integer not null,
                \"name\" varchar(50) not null default 'honza'
            );");
        $db->query("insert into $quotedTableId ([id], [name]) values (1, 'cz');");
        $db->query("insert into $quotedTableId ([id], [name]) values (2, 'en');");

        $this->_client->writeTableAsyncDirect($table_id, array(
            'dataWorkspaceId' => $workspace['id'],
            'dataTableName' => $tableId,
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

        $db->query("truncate table $quotedTableId");
        $db->query("alter table $quotedTableId ADD \"update\" varchar(64) NOT NULL DEFAULT '';");
        $db->query("insert into $quotedTableId values (1, 'cz', '');");
        $db->query("insert into $quotedTableId values (3, 'sk', 'newValue');");
        $db->query("insert into $quotedTableId values (4, 'jp', 'test');");

        $this->_client->writeTableAsyncDirect($table['id'], array(
            'dataWorkspaceId' => $workspace['id'],
            'dataTableName' => $tableId,
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

    public function testMetadataManipulationRestrictions()
    {
        // create workspace and source table in workspace
        $workspace = $this->initTestWorkspace(self::BACKEND_SYNAPSE);

        $tableId = 'metadata_columns';

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        $backend->dropTableIfExists($tableId);

        $connection = $workspace['connection'];
        $db = $this->getDbConnection($connection);

        $quotedTableId = $db->getDatabasePlatform()->quoteIdentifier(sprintf(
            '%s.%s',
            $connection['schema'],
            $tableId
        ));

        $db->query("create table $quotedTableId (
                    \"string\" varchar(16) not null default 'string'
                );");

        // create table from workspace
        $tableId = $this->_client->createTableAsyncDirect($this->getTestBucketId(self::STAGE_IN), array(
            'name' => 'metadata_columns',
            'dataWorkspaceId' => $workspace['id'],
            'dataTableName' => $tableId,
        ));

        $expectedStringMetadata = [
            'KBC.datatype.type' => 'VARCHAR',
            'KBC.datatype.nullable' => '',
            'KBC.datatype.basetype' => 'STRING',
            'KBC.datatype.length' => '16',
            'KBC.datatype.default' => '\'string\'',
        ];

        $metadata = new Metadata($this->_client);

        $columnId = sprintf('%s.%s', $tableId, 'string');

        $columnMetadataArray = $metadata->listColumnMetadata($columnId);
        $this->assertCount(5, $columnMetadataArray);

        $this->assertMetadata($expectedStringMetadata, $columnMetadataArray);

        $columnMetadata = reset($columnMetadataArray);

        // check that metadata from storage provider cannot be deleted
        try {
            $metadata->deleteColumnMetadata($columnId, $columnMetadata['id']);
        } catch (ClientException $e) {
            $this->assertSame(403, $e->getCode());
            $this->assertSame('Metadata with "storage" provider cannot be deleted by user.', $e->getMessage());
            $this->assertSame('storage.metadata.invalidProvider', $e->getStringCode());
        }

        // check that metadata with storage provider cannot be changed
        try {
            $metadata->postColumnMetadata(
                $columnId,
                Metadata::PROVIDER_STORAGE,
                [
                    [
                        'key' => 'test',
                        'value' => '1234',
                    ],
                    [
                        'key' => $columnMetadata['key'],
                        'value' => '1234',
                    ],
                ]
            );
        } catch (ClientException $e) {
            $this->assertSame(403, $e->getCode());
            $this->assertSame('Metadata with "storage" provider cannot be edited by user.', $e->getMessage());
            $this->assertSame('storage.metadata.invalidProvider', $e->getStringCode());
        }

        $this->assertSame($columnMetadataArray, $metadata->listColumnMetadata($columnId));
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
