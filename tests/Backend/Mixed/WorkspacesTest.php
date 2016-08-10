<?php
/**
 * Created by PhpStorm.
 * User: martinhalamicek
 * Date: 03/05/16
 * Time: 09:45
 */
namespace Keboola\Test\Backend\Mixed;

use Keboola\StorageApi\ClientException;
use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Workspaces;
use Keboola\Test\Backend\Workspaces\WorkspacesTestCase;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;

class WorkspacesTest extends WorkspacesTestCase
{
    public function testCreateWorkspaceForMysqlBackendShouldNotBeAllowed()
    {
        $workspaces = new Workspaces($this->_client);

        try {
            $workspaces->createWorkspace([
                'backend' => self::BACKEND_MYSQL,
            ]);
            $this->fail('Mysql workspace should not be created');
        } catch (ClientException $e) {
            $this->assertEquals('backend.notSupported', $e->getStringCode());
        }
    }

    /**
     * @dataProvider  workspaceBackendsData
     * @param $backend
     */
    public function testCreateWorkspaceParam($backend)
    {
        $workspaces = new Workspaces($this->_client);

        $workspace = $workspaces->createWorkspace([
            'backend' => $backend,
        ]);
        $this->assertEquals($backend, $workspace['connection']['backend']);

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $backend->createTable("mytable", ["amount" => ($workspace['connection']['backend'] === self::BACKEND_SNOWFLAKE) ? "NUMBER" : "INT"]);

    }

    /**
     * @dataProvider  workspaceBackendsData
     * @param $backend
     */
    public function testMixedBackendWorkspaceLoad($backend)
    {
        if ($this->_client->bucketExists("in.c-mixed-test-mysql")) {
            if ($this->_client->tableExists("in.c-mixed-test-mysql.languages")) {
                $this->_client->dropTable("in.c-mixed-test-mysql.languages");
            }
            $this->_client->dropBucket("in.c-mixed-test-mysql");
        }
        if ($this->_client->bucketExists("in.c-mixed-test-redshift")) {
            if ($this->_client->tableExists("in.c-mixed-test-redshift.languages")) {
                $this->_client->dropTable("in.c-mixed-test-redshift.languages");
            }
            $this->_client->dropBucket("in.c-mixed-test-redshift");
        }
        if ($this->_client->bucketExists("in.c-mixed-test-snowflake")) {
            if ($this->_client->tableExists("in.c-mixed-test-snowflake.languages")) {
                $this->_client->dropTable("in.c-mixed-test-snowflake.languages");
            }
            $this->_client->dropBucket("in.c-mixed-test-snowflake");
        }
        $mysqlBucketId = $this->_client->createBucket("mixed-test-mysql","in","",self::BACKEND_MYSQL);
        $redshiftBucketId = $this->_client->createBucket("mixed-test-redshift","in","",self::BACKEND_REDSHIFT);
        $snowflakeBucketId = $this->_client->createBucket("mixed-test-snowflake","in","",self::BACKEND_SNOWFLAKE);

        //setup test tables
        $mysqlTableId = $this->_client->createTable(
            $mysqlBucketId, 'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv')
        );

        //setup test tables
        $redshiftTableId = $this->_client->createTable(
            $redshiftBucketId, 'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv')
        );

        //setup test tables
        $snowflakeTableId = $this->_client->createTable(
            $snowflakeBucketId, 'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv')
        );

        $workspaces = new Workspaces($this->_client);

        $workspace = $workspaces->createWorkspace([
            'backend' => $backend,
        ]);
        try {
            $workspaces->loadWorkspaceData($workspace['id'], [
                "input" => [
                    [
                        "source" => $mysqlTableId,
                        "destination" => "languages"
                    ]
                ]
            ]);
            $this->fail("Loading data from mysql not yet supported");
        } catch (ClientException $e) {
            $this->assertEquals("workspaces.invalidBackendSource", $e->getStringCode());
        }

        try {
            $workspaces->loadWorkspaceData($workspace['id'], [
                "input" => [
                    [
                        "source" => $redshiftTableId,
                        "destination" => "languages"
                    ]
                ]
            ]);
            if ($backend === self::BACKEND_REDSHIFT) {
                $wsBackend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
                $data = $wsBackend->fetchAll("languages", \PDO::FETCH_ASSOC);
                $this->assertArrayEqualsSorted(Client::parseCsv(file_get_contents(__DIR__ . '/../../_data/languages.csv'), true, ",", '"'), $data, 'id');
            }
        } catch (ClientException $e) {
            if ($backend === self::BACKEND_SNOWFLAKE) {
                $this->assertEquals("workspaces.invalidBackendSource", $e->getStringCode());
            }
        }

        try {
            $workspaces->loadWorkspaceData($workspace['id'], [
                "input" => [
                    [
                        "source" => $snowflakeTableId,
                        "destination" => "languages"
                    ]
                ]
            ]);
            if ($backend === self::BACKEND_SNOWFLAKE) {
                $wsBackend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
                $data = $wsBackend->fetchAll("languages", \PDO::FETCH_ASSOC);
                $this->assertArrayEqualsSorted(Client::parseCsv(file_get_contents(__DIR__ . '/../../_data/languages.csv'), true, ",", '"'), $data, 'id');
            }
        } catch (ClientException $e) {
            if ($backend === self::BACKEND_REDSHIFT) {
                $this->assertEquals("workspaces.invalidBackendSource", $e->getStringCode());
            }
        }
    }

    public function workspaceBackendsData()
    {
        return [
            [self::BACKEND_REDSHIFT],
            [self::BACKEND_SNOWFLAKE]
        ];
    }

}