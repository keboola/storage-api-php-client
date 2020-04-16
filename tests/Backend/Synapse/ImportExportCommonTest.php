<?php



namespace Keboola\Test\Backend\Synapse;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Exception;
use Keboola\StorageApi\Workspaces;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;
use Keboola\Test\Backend\Workspaces\WorkspacesTestCase;
use Keboola\Test\Backend\CommonPart1\ImportExportCommonTest as CommonImportExportTest;

class ImportExportCommonTest extends CommonImportExportTest
{
    /**
     * @dataProvider tableImportData
     * @param $importFileName
     */
    public function testTableImportExport(CsvFile $importFile, $expectationsFileName, $colNames, $format = 'rfc', $createTableOptions = array())
    {
        $expectationsFile = __DIR__ . '/../../_data/' . $expectationsFileName;
        $tableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'languages-2', $importFile, $createTableOptions);

        $result = $this->_client->writeTable($tableId, $importFile);
        $table = $this->_client->getTable($tableId);

        $this->assertEmpty($result['warnings']);
        $this->assertEquals($colNames, array_values($result['importedColumns']), 'columns');
        $this->assertEmpty($result['transaction']);
        $this->assertNotEmpty($table['dataSizeBytes']);
        $this->assertNotEmpty($result['totalDataSizeBytes']);

        // @TODO not implemented yet
//        // compare data
//        $this->assertLinesEqualsSorted(file_get_contents($expectationsFile), $this->_client->getTableDataPreview($tableId, array(
//            'format' => $format,
//        )), 'imported data comparsion');
//
//        // incremental
//        $result = $this->_client->writeTable($tableId, $importFile, array(
//            'incremental' => true,
//        ));
//        $this->assertNotEmpty($result['totalDataSizeBytes']);
    }

    /**
     * @dataProvider tableImportData
     * @param $importFileName
     */
    public function testTableAsyncImportExport(CsvFile $importFile, $expectationsFileName, $colNames, $format = 'rfc', $createTableOptions = array())
    {
        $expectationsFile = __DIR__ . '/../../_data/' . $expectationsFileName;
        $tableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'languages-3', $importFile, $createTableOptions);

        $result = $this->_client->writeTableAsync($tableId, $importFile);
        $table = $this->_client->getTable($tableId);

        $this->assertEmpty($result['warnings']);
        $this->assertEquals($colNames, array_values($result['importedColumns']), 'columns');
        $this->assertEmpty($result['transaction']);
        $this->assertNotEmpty($table['dataSizeBytes']);
        $this->assertNotEmpty($result['totalDataSizeBytes']);

        // @TODO not implemented yet
//        // compare data
//        $this->assertLinesEqualsSorted(file_get_contents($expectationsFile), $this->_client->getTableDataPreview($tableId, array(
//            'format' => $format,
//        )), 'imported data comparsion');
//
//        // incremental
//
//        $result = $this->_client->writeTableAsync($tableId, $importFile, array(
//            'incremental' => true,
//        ));
//        $this->assertNotEmpty($result['totalDataSizeBytes']);
    }

    /**
     * @dataProvider incrementalImportPkDedupeData
     * @param $createFile
     * @param $primaryKey
     * @param $expectationFileAfterCreate
     * @param $incrementFile
     * @param $expectationFinal
     */
    public function testIncrementalImportPkDedupe($createFile, $primaryKey, $expectationFileAfterCreate, $incrementFile, $expectationFinal)
    {

        $tableId = $this->_client->createTableAsync($this->getTestBucketId(self::STAGE_IN), 'pk', $createFile, [
            'primaryKey' => $primaryKey,
        ]);

        // @TODO not implemented yet
//        $this->assertLinesEqualsSorted(file_get_contents($expectationFileAfterCreate), $this->_client->getTableDataPreview($tableId));

        $this->_client->writeTableAsync($tableId, $incrementFile, [
            'incremental' => true,
        ]);

        // @TODO not implemented yet
//        $this->assertLinesEqualsSorted(file_get_contents($expectationFinal), $this->_client->getTableDataPreview($tableId));
    }

    public function testTableImportCreateMissingColumns()
    {
        $filePath = __DIR__ . '/../../_data/languages.camel-case-columns.csv';
        $importFile = new CsvFile($filePath);
        $tableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'languages', $importFile);

        $extendedFile = __DIR__ . '/../../_data/languages-more-columns.csv';
        $result = $this->_client->writeTable($tableId, new CsvFile($extendedFile));
        $table = $this->_client->getTable($tableId);

        $this->assertEmpty($result['warnings']);
        $this->assertEquals(array('Id', 'Name', 'iso', 'Something'), array_values($result['importedColumns']), 'columns');
        $this->assertEmpty($result['transaction']);
        $this->assertNotEmpty($table['dataSizeBytes']);
        $this->assertNotEmpty($result['totalDataSizeBytes']);

        // @TODO not implemented yet
//        // compare data
//        $this->assertLinesEqualsSorted(file_get_contents($extendedFile), $this->_client->getTableDataPreview($tableId, array(
//            'format' => 'rfc',
//        )), 'imported data comparsion');
    }

    public function testTableImportFromString()
    {
        $tableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'languages', new \Keboola\Csv\CsvFile(__DIR__ . '/../../_data/languages-headers.csv'));

        $lines = '"id","name"';
        $lines .= "\n" . '"first","second"' . "\n";
        $this->_client->apiPost("storage/tables/$tableId/import", array(
            'dataString' => $lines,
        ));

        // @TODO not implemented yet
//        $this->assertEquals($lines, $this->_client->getTableDataPreview($tableId, array(
//            'format' => 'rfc',
//        )));
    }

    public function testTableAsyncExportRepeatedly()
    {
        $this->markTestSkipped('Exporting table table for Synapse backend is not supported yet');
    }
}
