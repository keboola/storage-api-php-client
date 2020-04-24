<?php



namespace Keboola\Test\Backend\Synapse;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Exception;
use Keboola\StorageApi\Workspaces;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;
use Keboola\Test\Backend\Workspaces\WorkspacesTestCase;
use Keboola\Test\Backend\CommonPart1\CreateTableTest as CommonCreateTableTest;

class CreateTableTest extends CommonCreateTableTest
{
    public function testRowNumberAmbiguity()
    {
        $importFile = __DIR__ . '/../../_data/column-name-row-number.csv';

        // create and import data into source table
        $tableId = $this->_client->createTable(
            $this->getTestBucketId(),
            'column-name-row-number',
            new CsvFile($importFile)
        );

//        // @TODO not implemented yet
//        // this used to fail because of the column named row_number
//        $this->_client->createTablePrimaryKey($tableId, ['id']);
//        $this->assertNotEmpty($tableId);
    }
}
