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
    public function setUp(): void
    {
        parent::setUp();
        $this->deleteAllWorkspaces();
    }

    /**
     * @dataProvider workspaceMixedBackendData
     * @param $backend
     * @param $bucketBackend
     */
    public function testLoadIncremental($backend, $bucketBackend): void
    {
        if ($this->_client->bucketExists('in.c-mixed-test-' . $bucketBackend)) {
            $this->_client->dropBucket(
                "in.c-mixed-test-{$bucketBackend}",
                [
                    'force' => true,
                ],
            );
        }

        $bucketId = $this->_client->createBucket("mixed-test-{$bucketBackend}", 'in', '', $bucketBackend);

        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace(['backend' => $backend], true);
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $importFile = __DIR__ . '/../../_data/languages-more-columns.csv';
        $tableId = $this->_client->createTableAsync(
            $bucketId,
            'languagesDetails',
            new CsvFile($importFile),
            ['primaryKey' => 'Id'],
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
        $this->assertEquals(2, $backend->countRows('languagesDetails'));
        $workspaceData = $backend->fetchAll('languagesDetails', \PDO::FETCH_ASSOC, '"primary" ASC');

        $this->assertEquals(['title', 'primary'], array_keys($workspaceData[0]));
        $expectedData = [
            [
                'title' => '- unchecked -',
                'primary' => '0',
            ],
            [
                'title' => 'czech',
                'primary' => '26',
            ],
        ];
        $this->assertEquals($expectedData, $workspaceData);

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
        $this->assertEquals(5, $backend->countRows('languagesDetails'));

        $workspaceData = $backend->fetchAll('languagesDetails', \PDO::FETCH_ASSOC, '"primary" ASC');

        $this->assertEquals(['title', 'primary'], array_keys($workspaceData[0]));
        $expectedData = [
            [
                'title' => '- unchecked -',
                'primary' => '0',
            ],
            [
                'title' => 'english',
                'primary'=> '1',
            ],
            [
                'title' => 'finnish',
                'primary' => '11',
            ],
            [
                'title' => 'french',
                'primary' => '24',
            ],
            [
                'title' => 'czech',
                'primary' => '26',
            ],
        ];
        $this->assertEquals($expectedData, $workspaceData);
    }

    /**
     * @dataProvider workspaceMixedBackendData
     * @param $backend
     * @param $bucketBackend
     */
    public function testLoadIncrementalWithColumnsReorder($backend, $bucketBackend): void
    {
        if ($this->_client->bucketExists('in.c-mixed-test-' . $bucketBackend)) {
            $this->_client->dropBucket(
                "in.c-mixed-test-{$bucketBackend}",
                [
                    'force' => true,
                ],
            );
        }

        $bucketId = $this->_client->createBucket("mixed-test-{$bucketBackend}", 'in', '', $bucketBackend);

        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace(['backend' => $backend], true);
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $importFile = __DIR__ . '/../../_data/languages-more-columns.csv';
        $tableId = $this->_client->createTableAsync(
            $bucketId,
            'languagesDetails',
            new CsvFile($importFile),
            ['primaryKey' => 'Id'],
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
        $this->assertEquals(2, $backend->countRows('languagesDetails'));

        $workspaceData = $backend->fetchAll('languagesDetails', \PDO::FETCH_ASSOC, '"primary" ASC');

        $this->assertEquals(['primary', 'title'], array_keys($workspaceData[0]));
        $expectedData = [
            [
                'primary' => '0',
                'title' => '- unchecked -',
            ],
            [
                'primary' => '26',
                'title' => 'czech',
            ],
        ];
        $this->assertEquals($expectedData, $workspaceData);

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
        $this->assertEquals(5, $backend->countRows('languagesDetails'));

        $workspaceData = $backend->fetchAll('languagesDetails', \PDO::FETCH_ASSOC, '"primary" ASC');

        $this->assertEquals(['primary', 'title'], array_keys($workspaceData[0]));
        $expectedData = [
            [
                'title' => '- unchecked -',
                'primary' => '0',
            ],
            [
                'title' => 'english',
                'primary'=> '1',
            ],
            [
                'title' => 'finnish',
                'primary' => '11',
            ],
            [
                'title' => 'french',
                'primary' => '24',
            ],
            [
                'title' => 'czech',
                'primary' => '26',
            ],
        ];
        $this->assertEquals($expectedData, $workspaceData);
    }

    public function workspaceMixedBackendData()
    {
        return [
            [self::BACKEND_SNOWFLAKE, self::BACKEND_REDSHIFT],
            [self::BACKEND_REDSHIFT, self::BACKEND_SNOWFLAKE],
        ];
    }
}
