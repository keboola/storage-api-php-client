<?php
/**
 * @author Erik Zigo <erik.zigo@keboola.com>
 */
namespace Keboola\Test\Backend\Mixed;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Workspaces;
use Keboola\Test\Backend\Workspaces\WorkspacesTestCase;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;

class WorkspacesRenameTest extends WorkspacesTestCase
{
    /**
     * @dataProvider workspaceMixedBackendData
     * @param $backend
     * @param $bucketBackend
     */
    public function testLoadIncremental($backend, $bucketBackend)
    {
        if ($this->_client->bucketExists("in.c-mixed-test-" . $bucketBackend)) {
            $this->_client->dropBucket(
                "in.c-mixed-test-{$bucketBackend}",
                [
                    'force' => true,
                ]
            );
        }

        $bucketId = $this->_client->createBucket("mixed-test-{$bucketBackend}", "in", "", $bucketBackend);

        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace(["backend" => $backend]);
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $importFile = __DIR__ . '/../../_data/languages-more-columns.csv';
        $tableId = $this->_client->createTable(
            $bucketId,
            'languagesDetails',
            new CsvFile($importFile),
            ['primaryKey' => 'Id']
        );

        // first load
        $options = [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'languagesDetails',
                    'whereColumn' => 'iso',
                    'whereValues' => ['dd', 'xx'],
                    'columns' => [
                        [
                            'source' => 'Id',
                            'destination' => 'primary',
                            'type' => 'integer',
                        ],
                        [
                            'source' => 'Name',
                            'destination' => 'title',
                            'type' => 'varchar',
                        ],
                    ],
                ],
            ],
        ];

        $workspaces->loadWorkspaceData($workspace['id'], $options);
        $this->assertEquals(2, $backend->countRows("languagesDetails"));

        foreach ($backend->fetchAll('languagesDetails', \PDO::FETCH_ASSOC) as $row) {
            $this->assertArrayHasKey('primary', $row);
            $this->assertArrayHasKey('title', $row);

            $this->assertTrue(is_numeric($row['primary']));
            $this->assertFalse(is_numeric($row['title']));
        }

        // second load
        $options = [
            'input' => [
                [
                    'incremental' => true,
                    'source' => $tableId,
                    'destination' => 'languagesDetails',
                    'whereColumn' => 'iso',
                    'whereValues' => ['ff', 'xx'],
                    'columns' => [
                        [
                            'source' => 'Id',
                            'destination' => 'primary',
                            'type' => 'integer',
                        ],
                        [
                            'source' => 'Name',
                            'destination' => 'title',
                            'type' => 'varchar',
                        ],
                    ],
                ],
            ],
        ];

        $workspaces->loadWorkspaceData($workspace['id'], $options);
        $this->assertEquals(5, $backend->countRows("languagesDetails"));

        foreach ($backend->fetchAll('languagesDetails', \PDO::FETCH_ASSOC) as $row) {
            $this->assertArrayHasKey('primary', $row);
            $this->assertArrayHasKey('title', $row);

            $this->assertTrue(is_numeric($row['primary']));
            $this->assertFalse(is_numeric($row['title']));
        }
    }

    /**
     * @dataProvider workspaceMixedBackendData
     * @param $backend
     * @param $bucketBackend
     */
    public function testLoadIncrementalWithColumnsReorder($backend, $bucketBackend)
    {
        if ($this->_client->bucketExists("in.c-mixed-test-" . $bucketBackend)) {
            $this->_client->dropBucket(
                "in.c-mixed-test-{$bucketBackend}",
                [
                    'force' => true,
                ]
            );
        }

        $bucketId = $this->_client->createBucket("mixed-test-{$bucketBackend}", "in", "", $bucketBackend);

        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace(["backend" => $backend]);
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $importFile = __DIR__ . '/../../_data/languages-more-columns.csv';
        $tableId = $this->_client->createTable(
            $bucketId,
            'languagesDetails',
            new CsvFile($importFile),
            ['primaryKey' => 'Id']
        );

        // first load
        $options = [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'languagesDetails',
                    'whereColumn' => 'iso',
                    'whereValues' => ['dd', 'xx'],
                    'columns' => [
                        [
                            'source' => 'Id',
                            'destination' => 'primary',
                            'type' => 'integer',
                        ],
                        [
                            'source' => 'Name',
                            'destination' => 'title',
                            'type' => 'varchar',
                        ],
                    ],
                ],
            ],
        ];

        $workspaces->loadWorkspaceData($workspace['id'], $options);
        $this->assertEquals(2, $backend->countRows("languagesDetails"));

        foreach ($backend->fetchAll('languagesDetails', \PDO::FETCH_ASSOC) as $row) {
            $this->assertArrayHasKey('primary', $row);
            $this->assertArrayHasKey('title', $row);

            $this->assertTrue(is_numeric($row['primary']));
            $this->assertFalse(is_numeric($row['title']));
        }

        // second load
        $options = [
            'input' => [
                [
                    'incremental' => true,
                    'source' => $tableId,
                    'destination' => 'languagesDetails',
                    'whereColumn' => 'iso',
                    'whereValues' => ['ff', 'xx'],
                    'columns' => [
                        [
                            'source' => 'Name',
                            'destination' => 'title',
                            'type' => 'varchar',
                        ],
                        [
                            'source' => 'Id',
                            'destination' => 'primary',
                            'type' => 'integer',
                        ],
                    ],
                ],
            ],
        ];

        $workspaces->loadWorkspaceData($workspace['id'], $options);
        $this->assertEquals(5, $backend->countRows("languagesDetails"));

        foreach ($backend->fetchAll('languagesDetails', \PDO::FETCH_ASSOC) as $row) {
            $this->assertArrayHasKey('primary', $row);
            $this->assertArrayHasKey('title', $row);

            $this->assertTrue(is_numeric($row['primary']));
            $this->assertFalse(is_numeric($row['title']));
        }
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
