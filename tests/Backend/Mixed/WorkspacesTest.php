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
     * @dataProvider  workspaceBackendData
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
     * @dataProvider  workspaceMixedBackendData
     * @param $backend
     */
    public function testMixedBackendWorkspaceLoad($backend, $bucketBackend)
    {
        if ($this->_client->bucketExists("in.c-mixed-test-" . $bucketBackend)) {
            if ($this->_client->tableExists("in.c-mixed-test-{$bucketBackend}.languages")) {
                $this->_client->dropTable("in.c-mixed-test-{$bucketBackend}.languages");
            }
            $this->_client->dropBucket("in.c-mixed-test-{$bucketBackend}");
        }
        $bucketId = $this->_client->createBucket("mixed-test-{$bucketBackend}","in","",$bucketBackend);

        //setup test table
        $this->_client->createTable(
            $bucketId, 'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv')
        );

        $workspaces = new Workspaces($this->_client);

        $workspace = $workspaces->createWorkspace([
            'backend' => $backend,
        ]);

        $options = [
            "input" => [
                [
                    "source" => "in.c-mixed-test-{$bucketBackend}.languages",
                    "destination" => "{$bucketBackend}_Languages"
                ]
            ]
        ];

        $workspaces->loadWorkspaceData($workspace['id'], $options);

        $wsBackend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $data = $wsBackend->fetchAll("{$bucketBackend}_Languages", \PDO::FETCH_ASSOC);

        $this->assertArrayEqualsSorted(Client::parseCsv(file_get_contents(__DIR__ . '/../../_data/languages.csv'), true, ",", '"'), $data, 'id');
    }

    public function workspaceBackendData()
    {
        return [
            [self::BACKEND_SNOWFLAKE],
            [self::BACKEND_REDSHIFT],
        ];
    }

    public function workspaceMixedBackendData()
    {
        return [
            [self::BACKEND_SNOWFLAKE, self::BACKEND_REDSHIFT],
            [self::BACKEND_SNOWFLAKE, self::BACKEND_MYSQL],
            [self::BACKEND_REDSHIFT, self::BACKEND_SNOWFLAKE],
            [self::BACKEND_REDSHIFT, self::BACKEND_MYSQL],
        ];
    }

}