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
    /**
     * @dataProvider tableCreateData
     * @param $createFile
     */
    public function testTableCreate($tableName, $createFile, $expectationFile, $async, $options = array())
    {
        $createMethod = $async ? 'createTableAsync' : 'createTable';
        $tableId = $this->_client->{$createMethod}(
            $this->getTestBucketId(self::STAGE_IN),
            $tableName,
            new CsvFile($createFile),
            $options
        );
        $table = $this->_client->getTable($tableId);

        $this->assertArrayHasKey('displayName', $table['bucket']);

        $expectationFileCsv = new CsvFile($expectationFile);

        $this->assertEquals($tableId, $table['id']);
        $this->assertEquals($tableName, $table['name']);
        $this->assertEquals($tableName, $table['displayName'], 'display name is same as name');
        $this->assertNotEmpty($table['created']);
        $this->assertNotEmpty($table['lastChangeDate']);
        $this->assertNotEmpty($table['lastImportDate']);
        $this->assertEquals($expectationFileCsv->getHeader(), $table['columns']);
        $this->assertEmpty($table['indexedColumns']);
        $this->assertNotEquals('0000-00-00 00:00:00', $table['created']);
        $this->assertNotEmpty($table['dataSizeBytes']);

//        // @TODO not implemented yet
//        $this->assertLinesEqualsSorted(
//            file_get_contents($expectationFile),
//            $this->_client->getTableDataPreview($tableId),
//            'initial data imported into table'
//        );

        $displayName = 'Romanov-display-name';
        $tableId = $this->_client->updateTable(
            $tableId,
            [
                'displayName' => $displayName,
            ]
        );

        $table = $this->_client->getTable($tableId);

        $this->assertEquals($displayName, $table['displayName']);

        // rename table to same name it already has should succeed
        $this->_client->updateTable(
            $tableId,
            [
                'displayName' => $displayName,
            ]
        );

        try {
            $tableId = $this->_client->{$createMethod}(
                $this->getTestBucketId(self::STAGE_IN),
                $displayName,
                new CsvFile($createFile),
                $options
            );
            $this->fail('Should fail');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals(
                sprintf(
                    'The table "%s" in the bucket already has the same display name "%s".',
                    $table['name'],
                    $displayName
                ),
                $e->getMessage()
            );
            $this->assertEquals('storage.buckets.tableAlreadyExists', $e->getStringCode());
        }

        try {
            $this->_client->updateTable(
                $tableId,
                [
                    'displayName' => '_wrong-display-name',
                ]
            );
            $this->fail('Should fail');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals(
                'Invalid data - displayName: Cannot start with underscore.',
                $e->getMessage()
            );
            $this->assertEquals('storage.tables.validation', $e->getStringCode());
            $this->assertEquals(400, $e->getCode());
        }

        $tableNameAnother = $tableName . '_another';
        $anotherTableId = $this->_client->{$createMethod}(
            $this->getTestBucketId(self::STAGE_IN),
            $tableNameAnother,
            new CsvFile($createFile),
            $options
        );
        try {
            $this->_client->updateTable(
                $anotherTableId,
                [
                    'displayName' => $displayName,
                ]
            );
            $this->fail('Renaming another table to existing displayname should fail');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals(
                sprintf(
                    'The table "%s" in the bucket already has the same display name "%s".',
                    $table['name'],
                    $displayName
                ),
                $e->getMessage()
            );
            $this->assertEquals('storage.buckets.tableAlreadyExists', $e->getStringCode());
            $this->assertEquals(400, $e->getCode());
        }
    }

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
