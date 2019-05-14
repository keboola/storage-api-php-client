<?php

declare(strict_types=1);

namespace Keboola\Test\Backend\Redshift;

use Keboola\StorageApi\Workspaces;
use Keboola\Test\Backend\Workspaces\WorkspacesTestCase;

class MetadataFromWorkspaceTest extends WorkspacesTestCase
{
    public function testCreateTableFromWorkspace()
    {
        // create workspace and source table in workspace
        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace(["backend" => "redshift"]);
        $connection = $workspace['connection'];
        $db = $this->getDbConnection($connection);
        $db->query("create table \"test.Languages3\" (
                    \"Id\" varchar not null,
                    \"Name\" varchar null
                );");
        // create table from workspace
        $tableId = $this->_client->createTableAsyncDirect($this->getTestBucketId(self::STAGE_IN), array(
            'name' => 'languages3',
            'dataWorkspaceId' => $workspace['id'],
            'dataTableName' => 'test.Languages3',
        ));
        $expectedIdMetadata = [
            'KBC.datatype.type' => 'varchar',
            'KBC.datatype.nullable' => '',
            'KBC.datatype.basetype' => 'STRING',
            'KBC.datatype.length' => '256',
        ];
        $expectedNameMetadata = [
            'KBC.datatype.type' => 'varchar',
            'KBC.datatype.nullable' => '1',
            'KBC.datatype.basetype' => 'STRING',
            'KBC.datatype.length' => '256',
        ];
        // check that the new table has the correct metadata
        $table = $this->_client->getTable($tableId);
        $this->assertEquals([], $table['metadata']);
        $this->assertArrayHasKey('id', $table['columnMetadata']);
        $this->assertMetadata($expectedIdMetadata, $table['columnMetadata']['id']);
        $this->assertArrayHasKey('name', $table['columnMetadata']);
        $this->assertMetadata($expectedNameMetadata, $table['columnMetadata']['name']);
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
        $workspace = $workspaces->createWorkspace(["backend" => "redshift"]);
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
            'KBC.datatype.type' => 'int4',
            'KBC.datatype.nullable' => '',
            'KBC.datatype.basetype' => 'STRING',
        ];
        $expectedNameMetadata = [
            'KBC.datatype.type' => 'varchar',
            'KBC.datatype.nullable' => '',
            'KBC.datatype.basetype' => 'STRING',
            'KBC.datatype.length' => '256',
            'KBC.datatype.default' => '\'honza\'',
        ];
        $expectedUpdateMetadata = [
            'KBC.datatype.type' => 'varchar',
            'KBC.datatype.nullable' => '1',
            'KBC.datatype.basetype' => 'STRING',
            'KBC.datatype.length' => '256',
        ];
        // check that the new table has the correct metadata
        $table = $this->_client->getTable($table['id']);
        $this->assertEquals([], $table['metadata']);
        $this->assertArrayHasKey('id', $table['columnMetadata']);
        $this->assertMetadata($expectedIdMetadata, $table['columnMetadata']['id']);
        $this->assertArrayHasKey('name', $table['columnMetadata']);
        $this->assertMetadata($expectedNameMetadata, $table['columnMetadata']['name']);
        $this->assertArrayHasKey('update', $table['columnMetadata']);
        $this->assertMetadata($expectedUpdateMetadata, $table['columnMetadata']['update']);

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
        $expectedNewColMetadata = [
            'KBC.datatype.type' => 'TEXT',
            'KBC.datatype.nullable' => '1',
            'KBC.datatype.basetype' => 'STRING',
            'KBC.datatype.length' => '64',
            'KBC.datatype.default' => '',
        ];
        $table = $this->_client->getTable($table['id']);
        $this->assertEquals([], $table['metadata']);
        $this->assertArrayHasKey("id", $table['columnMetadata']);
        $this->assertArrayHasKey("name", $table['columnMetadata']);
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
