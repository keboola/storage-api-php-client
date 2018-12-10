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
			\"Id\" integer not null,
			\"Name\" varchar not null
		);");
        $db->query("insert into \"test.Languages3\" (\"Id\", \"Name\") values (1, 'cz'), (2, 'en');");

        // create table from workspace
        $tableId = $this->_client->createTableAsyncDirect($this->getTestBucketId(self::STAGE_IN), array(
            'name' => 'languages3',
            'dataWorkspaceId' => $workspace['id'],
            'dataTableName' => 'test.Languages3',
        ));

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
			\"Name\" varchar not null,
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
        $db->query("alter table \"test.Languages3\" ADD COLUMN \"new_col\" varchar");
        $db->query("insert into \"test.Languages3\" values (1, 'cz', '1', null), (3, 'sk', '1', 'newValue');");

        $this->_client->writeTableAsyncDirect($table['id'], array(
            'dataWorkspaceId' => $workspace['id'],
            'dataTableName' => 'test.Languages3',
            'incremental' => true,
        ));

        $expected = array(
            '"Id","Name","update","new_col"',
            '"1","cz","1",""',
            '"2","en","",""',
            '"3","sk","1","newValue"',
        );
        $this->assertLinesEqualsSorted(implode("\n", $expected) . "\n", $this->_client->getTableDataPreview($table['id'], array(
            'format' => 'rfc',
        )), 'new  column added');
    }

    public function testCreateTableFromWorkspaceWithoutHandleAsyncTaskSuccess()
    {
        // create workspace and source table in workspace
        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace();
        $connection = $workspace['connection'];
        $db = $this->getDbConnection($connection);
        $db->query("create table \"test.Languages1\" (
			\"Id\" integer not null,
			\"Name\" varchar not null
		);");
        $db->query("insert into \"test.Languages1\" (\"Id\", \"Name\") values (1, 'cz'), (2, 'en');");
        $db->query("create table \"test.Languages2\" (
			\"Id\" integer not null,
			\"Name\" varchar not null
		);");
        $db->query("insert into \"test.Languages2\" (\"Id\", \"Name\") values (3, 'fr');");

        $job1 = $this->_client->createTableAsyncDirect(
            $this->getTestBucketId(self::STAGE_IN),
            array(
                'name' => 'languages1',
                'dataWorkspaceId' => $workspace['id'],
                'dataTableName' => 'test.Languages1',
            ),
            false
        );

        $job2 = $this->_client->createTableAsyncDirect(
            $this->getTestBucketId(self::STAGE_IN),
            array(
                'name' => 'languages2',
                'dataWorkspaceId' => $workspace['id'],
                'dataTableName' => 'test.Languages2',
            ),
            false
        );

        $results = $this->_client->handleAsyncTasks([$job1, $job2]);

        $this->assertCount(2, $results);
        $this->assertTrue($this->_client->tableExists($results[0]['results']['id']));
        $this->assertTrue($this->_client->tableExists($results[1]['results']['id']));
    }


    public function testCreateTableFromWorkspaceWithoutHandleAsyncTaskError()
    {
        // create workspace and source table in workspace
        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace();
        $connection = $workspace['connection'];
        $db = $this->getDbConnection($connection);
        $db->query("create table \"test.Languages1\" (
			\"Id\" integer not null,
			\"Name\" varchar not null
		);");
        $db->query("insert into \"test.Languages1\" (\"Id\", \"Name\") values (1, 'cz'), (2, 'en');");
        $db->query("create table \"test.Languages2\" (
			\"_Id\" integer not null,
			\"Name\" varchar not null
		);");
        $db->query("insert into \"test.Languages2\" (\"_Id\", \"Name\") values (3, 'fr');");

        $job1 = $this->_client->createTableAsyncDirect(
            $this->getTestBucketId(self::STAGE_IN),
            array(
                'name' => 'languages1',
                'dataWorkspaceId' => $workspace['id'],
                'dataTableName' => 'test.Languages1',
            ),
            false
        );

        $job2 = $this->_client->createTableAsyncDirect(
            $this->getTestBucketId(self::STAGE_IN),
            array(
                'name' => 'languages2',
                'dataWorkspaceId' => $workspace['id'],
                'dataTableName' => 'test.Languages2',
            ),
            false
        );

        try {
            $this->_client->handleAsyncTasks([$job1, $job2]);
            $this->fail('Exception expected');
        } catch (ClientException $e) {
            $this->assertEquals('storage.invalidColumns', $e->getStringCode());
        }
        $this->assertTrue($this->_client->tableExists($this->getTestBucketId(self::STAGE_IN) . '.languages1'));
        $this->assertFalse($this->_client->tableExists($this->getTestBucketId(self::STAGE_IN) . '.languages2'));
    }


    public function testImportTableFromWorkspaceWithoutHandleAsyncTaskSuccess()
    {
        // create workspace and source table in workspace
        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace();
        $connection = $workspace['connection'];
        $db = $this->getDbConnection($connection);
        $db->query("create table \"test.Languages1\" (
			\"Id\" integer not null,
			\"Name\" varchar not null
		);");
        $db->query("insert into \"test.Languages1\" (\"Id\", \"Name\") values (1, 'cz'), (2, 'en');");
        $db->query("create table \"test.Languages2\" (
			\"Id\" integer not null,
			\"Name\" varchar not null
		);");
        $db->query("insert into \"test.Languages2\" (\"Id\", \"Name\") values (3, 'fr');");


        $table1 = $this->_client->apiPost("storage/buckets/" . $this->getTestBucketId(self::STAGE_IN) . "/tables", array(
            'dataString' => 'Id,Name',
            'name' => 'languages1',
            'primaryKey' => 'Id',
        ));
        $table2 = $this->_client->apiPost("storage/buckets/" . $this->getTestBucketId(self::STAGE_IN) . "/tables", array(
            'dataString' => 'Id,Name',
            'name' => 'languages2',
            'primaryKey' => 'Id',
        ));

        $job1 = $this->_client->writeTableAsyncDirect(
            $table1['id'],
            array(
                'dataWorkspaceId' => $workspace['id'],
                'dataTableName' => 'test.Languages1'
            ),
            false
        );
        $job2 = $this->_client->writeTableAsyncDirect(
            $table2['id'],
            array(
                'dataWorkspaceId' => $workspace['id'],
                'dataTableName' => 'test.Languages2'
            ),
            false
        );

        $results = $this->_client->handleAsyncTasks([$job1, $job2]);

        $this->assertCount(2, $results);
        $this->assertEquals(2, $results[0]['results']['totalRowsCount']);
        $this->assertEquals(1, $results[1]['results']['totalRowsCount']);
    }


    public function testImportTableFromWorkspaceWithoutHandleAsyncTaskError()
    {
        // create workspace and source table in workspace
        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace();
        $connection = $workspace['connection'];
        $db = $this->getDbConnection($connection);
        $db->query("create table \"test.Languages1\" (
			\"Id\" integer not null,
			\"Name\" varchar not null
		);");
        $db->query("insert into \"test.Languages1\" (\"Id\", \"Name\") values (1, 'cz'), (2, 'en');");
        $db->query("create table \"test.Languages2\" (
			\"_Id\" integer not null,
			\"Name\" varchar not null
		);");
        $db->query("insert into \"test.Languages2\" (\"_Id\", \"Name\") values (3, 'fr');");


        $table1 = $this->_client->apiPost("storage/buckets/" . $this->getTestBucketId(self::STAGE_IN) . "/tables", array(
            'dataString' => 'Id,Name',
            'name' => 'languages1',
            'primaryKey' => 'Id',
        ));
        $table2 = $this->_client->apiPost("storage/buckets/" . $this->getTestBucketId(self::STAGE_IN) . "/tables", array(
            'dataString' => 'Id,Name',
            'name' => 'languages2',
            'primaryKey' => 'Id',
        ));

        $job1 = $this->_client->writeTableAsyncDirect(
            $table1['id'],
            array(
                'dataWorkspaceId' => $workspace['id'],
                'dataTableName' => 'test.Languages1'
            ),
            false
        );
        $job2 = $this->_client->writeTableAsyncDirect(
            $table2['id'],
            array(
                'dataWorkspaceId' => $workspace['id'],
                'dataTableName' => 'test.Languages2'
            ),
            false
        );

        try {
            $this->_client->handleAsyncTasks([$job1, $job2]);
            $this->fail('Exception expected');
        } catch (ClientException $e) {
            $this->assertEquals('storage.invalidColumns', $e->getStringCode());
        }

        $table1Info = $this->_client->getTable($table1['id']);
        $table2Info = $this->_client->getTable($table2['id']);

        $this->assertEquals(2, $table1Info['rowsCount']);
        $this->assertEquals(0, $table2Info['rowsCount']);
    }
}
