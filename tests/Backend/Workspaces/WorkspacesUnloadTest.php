<?php
/**
 *
 * User: Martin Halamíček
 * Date: 16.5.12
 * Time: 11:46
 *
 */

namespace Keboola\Test\Backend\Workspaces;

use Keboola\Db\Import\Snowflake\Connection;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Exception;
use Keboola\StorageApi\Workspaces;
use Keboola\Test\Common\MetadataTest;
use Keboola\Test\StorageApiTestCase;

class WorkspacesUnloadTest extends WorkspacesTestCase
{
    public function testCreateTableFromWorkspace()
    {
        // create workspace and source table in workspace
        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace();

        $connection = $workspace['connection'];

        $db = $this->getDbConnection($connection);

        $db->query("create table \"test.Languages3\" (
			\"Id\" varchar not null,
			\"Name\" varchar null
		);");
        $db->query("insert into \"test.Languages3\" (\"Id\", \"Name\") values (1, 'cz'), (2, 'en');");

        // create table from workspace
        $tableId = $this->_client->createTableAsyncDirect($this->getTestBucketId(self::STAGE_IN), array(
            'name' => 'languages3',
            'dataWorkspaceId' => $workspace['id'],
            'dataTableName' => 'test.Languages3',
        ));

        $expectedIdMetadata = [
            'KBC.datatype.type' => 'TEXT',
            'KBC.datatype.nullable' => '',
            'KBC.datatype.basetype' => 'STRING',
            'KBC.datatype.length' => '16777216',
            'KBC.datatype.default' => '',
        ];
        $expectedNameMetadata = [
            'KBC.datatype.type' => 'TEXT',
            'KBC.datatype.nullable' => '1',
            'KBC.datatype.basetype' => 'STRING',
            'KBC.datatype.length' => '16777216',
            'KBC.datatype.default' => '',
        ];
        // check that the new table has the correct metadata
        $table = $this->_client->getTable($tableId);
        $this->assertEquals([], $table['metadata']);
        $this->assertArrayHasKey('Id', $table['columnMetadata']);
        $this->assertMetadata($expectedIdMetadata, $table['columnMetadata']['Id']);
        $this->assertArrayHasKey('Name', $table['columnMetadata']);
        $this->assertMetadata($expectedNameMetadata, $table['columnMetadata']['Name']);

        $expected = array(
            ($connection['backend'] === parent::BACKEND_REDSHIFT) ? '"id","name"' : '"Id","Name"',
            '"1","cz"',
            '"2","en"',
        );

        $this->assertLinesEqualsSorted(implode("\n", $expected) . "\n", $this->_client->getTableDataPreview($tableId, array(
            'format' => 'rfc',
        )), 'imported data comparsion');
    }

    public function testCreateTableFromWorkspaceWithInvalidColumnNames()
    {
        // create workspace and source table in workspace
        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace();

        $connection = $workspace['connection'];

        $db = $this->getDbConnection($connection);

        $db->query("create table \"test.Languages3\" (
			\"_Id\" integer not null,
			\"Name\" varchar not null
		);");
        $db->query("insert into \"test.Languages3\" (\"_Id\", \"Name\") values (1, 'cz'), (2, 'en');");

        try {
            $this->_client->createTableAsyncDirect($this->getTestBucketId(self::STAGE_IN), array(
                'name' => 'languages3',
                'dataWorkspaceId' => $workspace['id'],
                'dataTableName' => 'test.Languages3',
            ));
            $this->fail('Table should not be created');
        } catch (ClientException $e) {
            $this->assertEquals('storage.invalidColumns', $e->getStringCode());
            $this->assertContains('_Id', $e->getMessage(), '', true);
        }
    }

    public function testImportFromWorkspaceWithInvalidColumnNames()
    {
        // create workspace and source table in workspace
        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace();

        $connection = $workspace['connection'];

        $db = $this->getDbConnection($connection);

        $db->query("create table \"test.Languages3\" (
			\"Id\" integer not null,
			\"Name\" varchar not null,
			\"_update\" varchar not null 
		);");
        $db->query("insert into \"test.Languages3\" (\"Id\", \"Name\", \"_update\") values (1, 'cz', 'x'), (2, 'en', 'z');");

        $table = $this->_client->apiPost("storage/buckets/" . $this->getTestBucketId(self::STAGE_IN) . "/tables", array(
            'dataString' => 'Id,Name',
            'name' => 'languages',
            'primaryKey' => 'Id',
        ));

        try {
            $this->_client->writeTableAsyncDirect($table['id'], array(
                'dataWorkspaceId' => $workspace['id'],
                'dataTableName' => 'test.Languages3',
                'incremental' => true,
            ));
            $this->fail('Table should not be imported');
        } catch (ClientException $e) {
            $this->assertEquals('storage.invalidColumns', $e->getStringCode());
            $this->assertContains('_update', $e->getMessage());
        }
    }

    public function testCopyImport()
    {
        $table = $this->_client->apiPost("storage/buckets/" . $this->getTestBucketId(self::STAGE_IN) . "/tables", array(
            'dataString' => 'Id,Name,update',
            'name' => 'languages',
            'primaryKey' => 'Id',
        ));

        // create workspace and source table in workspace
        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace();

        $connection = $workspace['connection'];

        $db = $this->getDbConnection($connection);

        $db->query("create table \"test.Languages3\" (
			\"Id\" integer not null,
			\"Name\" varchar not null default 'honza',
			\"update\" varchar
		);");

        $db->query("insert into \"test.Languages3\" (\"Id\", \"Name\") values (1, 'cz'), (2, 'en');");

        $this->_client->writeTableAsyncDirect($table['id'], array(
            'dataWorkspaceId' => $workspace['id'],
            'dataTableName' => 'test.Languages3',
        ));

        $expected = array(
            '"Id","Name","update"',
            '"1","cz",""',
            '"2","en",""',
        );
        $this->assertLinesEqualsSorted(implode("\n", $expected) . "\n", $this->_client->getTableDataPreview($table['id'], array(
            'format' => 'rfc',
        )), 'imported data comparsion');

        // check the created metadata
        $expectedIdMetadata = [
            'KBC.datatype.type' => 'NUMBER',
            'KBC.datatype.nullable' => '',
            'KBC.datatype.basetype' => 'NUMERIC',
            'KBC.datatype.length' => '38,0',
            'KBC.datatype.default' => '',
        ];
        $expectedNameMetadata = [
            'KBC.datatype.type' => 'TEXT',
            'KBC.datatype.nullable' => '',
            'KBC.datatype.basetype' => 'STRING',
            'KBC.datatype.length' => '16777216',
            'KBC.datatype.default' => '\'honza\'',
        ];
        $expectedUpdateMetadata = [
            'KBC.datatype.type' => 'TEXT',
            'KBC.datatype.nullable' => '1',
            'KBC.datatype.basetype' => 'STRING',
            'KBC.datatype.length' => '16777216',
            'KBC.datatype.default' => '',
        ];

        // check that the new table has the correct metadata
        $table = $this->_client->getTable($table['id']);
        $this->assertEquals([], $table['metadata']);
        $this->assertArrayHasKey('Id', $table['columnMetadata']);
        $this->assertMetadata($expectedIdMetadata, $table['columnMetadata']['Id']);
        $this->assertArrayHasKey('Name', $table['columnMetadata']);
        $this->assertMetadata($expectedNameMetadata, $table['columnMetadata']['Name']);
        $this->assertArrayHasKey('update', $table['columnMetadata']);
        $this->assertMetadata($expectedUpdateMetadata, $table['columnMetadata']['update']);

        $db->query("truncate \"test.Languages3\"");
        $db->query("insert into \"test.Languages3\" values (1, 'cz', '1'), (3, 'sk', '1');");

        $this->_client->writeTableAsyncDirect($table['id'], array(
            'dataWorkspaceId' => $workspace['id'],
            'dataTableName' => 'test.Languages3',
            'incremental' => true,
        ));

        $expected = array(
            '"Id","Name","update"',
            '"1","cz","1"',
            '"2","en",""',
            '"3","sk","1"',
        );
        $this->assertLinesEqualsSorted(implode("\n", $expected) . "\n", $this->_client->getTableDataPreview($table['id'], array(
            'format' => 'rfc',
        )), 'previously null column updated');

        $db->query("truncate table \"test.Languages3\"");
        $db->query("alter table \"test.Languages3\" ADD COLUMN \"new_col\" varchar(64)");
        $db->query("alter table \"test.Languages3\" RENAME TO \"test.Languages4\"");
        $db->query("insert into \"test.Languages4\" values " .
                    "(1, 'cz', '1', null)," .
                    " (3, 'sk', '1', 'newValue')," .
                    " (4, 'jp','1', 'test');");

        $this->_client->writeTableAsyncDirect($table['id'], array(
            'dataWorkspaceId' => $workspace['id'],
            'dataTableName' => 'test.Languages4',
            'incremental' => true,
        ));

        $expected = array(
            '"Id","Name","update","new_col"',
            '"1","cz","1",""',
            '"2","en","",""',
            '"3","sk","1","newValue"',
            '"4","jp","1","test"',
        );
        $this->assertLinesEqualsSorted(implode("\n", $expected) . "\n", $this->_client->getTableDataPreview($table['id'], array(
            'format' => 'rfc',
        )), 'new  column added');

        $expectedMetadata = [
            'KBC.name' => 'test.Languages4',
            'KBC.schema_name' => 'WORKSPACE_' . $workspace['id'],
            'KBC.rows' => '3',
            'KBC.bytes' => '1024',
            'KBC.owner' => $connection['user'],
        ];
        $expectedNewColMetadata = [
            'KBC.datatype.type' => 'TEXT',
            'KBC.datatype.nullable' => '1',
            'KBC.datatype.basetype' => 'STRING',
            'KBC.datatype.length' => '64',
            'KBC.datatype.default' => '',
        ];
        $table = $this->_client->getTable($table['id']);
        $this->assertMetadata($expectedMetadata, $table['metadata']);
        $this->assertArrayHasKey("Id", $table['columnMetadata']);
        $this->assertArrayHasKey("Name", $table['columnMetadata']);
        $this->assertArrayHasKey("update", $table['columnMetadata']);
        $this->assertArrayHasKey("new_col", $table['columnMetadata']);
        $this->assertMetadata($expectedNewColMetadata, $table['columnMetadata']['new_col']);
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
            $this->assertEquals('storage', $data['provider']);
        }
    }
}
