<?php
/**
 * Created by PhpStorm.
 * User: martinhalamicek
 * Date: 03/05/16
 * Time: 09:45
 */
namespace Keboola\Test\Backend\Mixed;

use Keboola\Db\Import\Snowflake\Connection;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Workspaces;
use Keboola\Test\StorageApiTestCase;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;

class WorkspacesTest extends StorageApiTestCase
{
    public function setUp()
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();
        $this->clearWorkspaces();
    }

    private function clearWorkspaces()
    {
        $workspaces = new Workspaces($this->_client);
        foreach ($workspaces->listWorkspaces() as $workspace) {
            $workspaces->deleteWorkspace($workspace['id']);
        }
    }

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

        $backend->createTable("mytable", ["amount" => "NUMBER"]);

    }

    /**
     * @dataProvider  workspaceBackendsData
     * @param $backend
     */
    public function testMixedBackendWorkspaceLoad($backend, $bucketBackend)
    {
        if ($this->_client->bucketExists("in.c-mixed-test-mysql")) {
            $this->_client->dropBucket("in.c-mixed-test-mysql");
        }
        if ($this->_client->bucketExists("in.c-mixed-test-redshift")) {
            $this->_client->dropBucket("in.c-mixed-test-redshift");
        }
        if ($this->_client->bucketExists("in.c-mixed-test-snowflake")) {
            $this->_client->dropBucket("in.c-mixed-test-snowflake");
        }
        $this->_client->createBucket("mixed-test-mysql","in","",self::BACKEND_MYSQL);
        $this->_client->createBucket("mixed-test-redshift","in","",self::BACKEND_REDSHIFT);
        $this->_client->createBucket("mixed-test-snowflake","in","",self::BACKEND_SNOWFLAKE);



        $workspaces = new Workspaces($this->_client);

        $workspace = $workspaces->createWorkspace([
            'backend' => $backend,
        ]);

        if ($backend === self::BACKEND_REDSHIFT) {

        }

    }

    public function workspaceBackendsData()
    {
        return [
            [self::BACKEND_REDSHIFT, self::BACKEND_SNOWFLAKE],
            [self::BACKEND_SNOWFLAKE, self::BACKEND_MYSQL]
        ];
    }

}