<?php
/**
 * Created by PhpStorm.
 * User: martinhalamicek
 * Date: 03/05/16
 * Time: 09:45
 */
namespace Keboola\Test\Backend\Workspaces;
use Keboola\StorageApi\Workspaces;
use Keboola\Csv\CsvFile;
use Keboola\Test\StorageApiTestCase;

class WorkspacesTest extends StorageApiTestCase
{
    public function setUp()
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();
    }


    public function testWorkspaceCreate()
    {
        // create some table
        $sourceTableId = $this->_client->createTable(
            $this->getTestBucketId(),
            'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv'),
            array(
                'primaryKey' => 'id'
            )
        );
        
        $workspaces = new Workspaces($this->_client);

        $workspace = $workspaces->createWorkspace();
        var_dump($workspace);
    }


}