<?php
/**
 * Created by JetBrains PhpStorm.
 * User: martinhalamicek
 * Date: 22/05/14
 * Time: 16:38
 * To change this template use File | Settings | File Templates.
 */

namespace Keboola\Test\Backend\CommonPart1;

use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Workspaces;
use Keboola\Test\StorageApiTestCase;
use Keboola\StorageApi\Client;
use Keboola\Csv\CsvFile;

class ImportExportCommonTest extends StorageApiTestCase
{

    public function setUp()
    {
        parent::setUp();
        $this->initEmptyTestBucketsForParallelTests();
    }

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

        // compare data
        $this->assertLinesEqualsSorted(file_get_contents($expectationsFile), $this->_client->getTableDataPreview($tableId, array(
            'format' => $format,
        )), 'imported data comparsion');

        // incremental
        $result = $this->_client->writeTable($tableId, $importFile, array(
            'incremental' => true,
        ));
        $this->assertNotEmpty($result['totalDataSizeBytes']);
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

        // compare data
        $this->assertLinesEqualsSorted(file_get_contents($expectationsFile), $this->_client->getTableDataPreview($tableId, array(
            'format' => $format,
        )), 'imported data comparsion');

        // incremental

        $result = $this->_client->writeTableAsync($tableId, $importFile, array(
            'incremental' => true,
        ));
        $this->assertNotEmpty($result['totalDataSizeBytes']);
    }


    public function tableImportData()
    {
        return array(
            array(new CsvFile(__DIR__ . '/../../_data/languages.csv'), 'languages.csv', array('id', 'name')),
            array(new CsvFile(__DIR__ . '/../../_data/languages.encoding.csv'), 'languages.encoding.csv', array('id', 'name')),
            array(new CsvFile(__DIR__ . '/../../_data/languages.with-duplicates.csv'), 'languages.csv', array('id', 'name'), 'rfc', array(
                'primaryKey' => 'id,name',
            )),

            array(new CsvFile(__DIR__ . '/../../_data/languages.special-column-names.csv'), 'languages.special-column-names.csv', array('Id', 'queryId', 'columnEndingWith_'), 'rfc', array(
                'primaryKey' => 'Id,queryId,columnEndingWith_',
            )),


            array(new CsvFile(__DIR__ . '/../../_data/languages.utf8.bom.csv'), 'languages.csv', array('id', 'name')),

            array(new CsvFile(__DIR__ . '/../../_data/languages.csv.gz'), 'languages.csv', array('id', 'name')),

            array(new CsvFile(__DIR__ . '/../../_data/escaping.csv'), 'escaping.standard.out.csv', array('col1', 'col2_with_space')),

            array(new CsvFile(__DIR__ . '/../../_data/escaping.nl-last-row.csv'), 'escaping.standard.out.csv', array('col1', 'col2_with_space')),

        );
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

        $this->assertLinesEqualsSorted(file_get_contents($expectationFileAfterCreate), $this->_client->getTableDataPreview($tableId));

        $this->_client->writeTableAsync($tableId, $incrementFile, [
            'incremental' => true,
        ]);
        $this->assertLinesEqualsSorted(file_get_contents($expectationFinal), $this->_client->getTableDataPreview($tableId));
    }

    public function incrementalImportPkDedupeData()
    {
        return [
            [
                new CsvFile(__DIR__ . '/../../_data/pk.simple.csv'),
                'id',
                new CsvFile(__DIR__ . '/../../_data/pk.simple.loaded.csv'),
                new CsvFile(__DIR__ . '/../../_data/pk.simple.increment.csv'),
                new CsvFile(__DIR__ . '/../../_data/pk.simple.increment.loaded.csv'),
            ],
            [
                new CsvFile(__DIR__ . '/../../_data/pk.multiple.csv'),
                'id,sub_id',
                new CsvFile(__DIR__ . '/../../_data/pk.multiple.loaded.csv'),
                new CsvFile(__DIR__ . '/../../_data/pk.multiple.increment.csv'),
                new CsvFile(__DIR__ . '/../../_data/pk.multiple.increment.loaded.csv'),
            ]
        ];
    }

    public function testTableImportColumnsCaseInsensitive()
    {
        $tokenData = $this->_client->verifyToken();
        if (in_array($tokenData['owner']['defaultBackend'], [self::BACKEND_SNOWFLAKE, self::BACKEND_SYNAPSE])) {
            return;
        }

        $importFile = new CsvFile(__DIR__ . '/../../_data/languages.csv');
        $tableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'languages', $importFile);

        $result = $this->_client->writeTableAsync($tableId, new CsvFile(__DIR__ . '/../../_data/languages.camel-case-columns.csv'));

        $table = $this->_client->getTable($tableId);
        $this->assertEquals($importFile->getHeader(), $table['columns']);
    }

    public function testTableImportCaseSensitiveThrowsUserError()
    {
        $tokenData = $this->_client->verifyToken();
        if (in_array($tokenData['owner']['defaultBackend'], [self::BACKEND_REDSHIFT, self::BACKEND_SYNAPSE])) {
            $this->markTestSkipped("Test case-sensitivity columns name only for snowflake");
        }

        $importFile = new CsvFile(__DIR__ . '/../../_data/languages.csv');
        $tableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'languages-case-sensitive', $importFile);

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage(
            'Some columns are missing in the csv file. Missing columns: id,name. '
            .'Expected columns: id,name. Please check if the expected delimiter "," is used in the csv file.'
        );
        
        $this->_client->writeTableAsync($tableId, new CsvFile(__DIR__ . '/../../_data/languages.camel-case-columns.csv'), ['incremental' => true]);
    }

    /**
     * @dataProvider tableImportInvalidData
     * @expectedException \Keboola\StorageApi\ClientException
     */
    public function testTableInvalidImport($languagesFile)
    {
        $importCsvFile = new CsvFile(__DIR__ . '/../../_data/' . $languagesFile);
        $tableId = $this->_client->createTable($this->getTestBucketId(), 'languages', new CsvFile(__DIR__ . '/../../_data/languages.csv'));

        $this->_client->writeTable($tableId, $importCsvFile);
    }

    public function tableImportInvalidData()
    {
        return array(
            array('languages.invalid.csv'),
            array('languages.invalid.gzip'),
            array('languages.invalid.zip'),
            array('languages.invalid.duplicateColumns.csv'),
        );
    }

    public function testTableImportNotExistingFile()
    {
        try {
            $this->_client->writeTable($this->getTestBucketId() . '.languages', new CsvFile('invalid.csv'));
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('fileNotReadable', $e->getStringCode());
        }
    }

    public function testTableImportInvalidCsvParams()
    {
        try {
            $this->_client->apiPost("buckets/{$this->getTestBucketId(self::STAGE_IN)}/tables", [
                'dataString' => 'id,name',
                'delimiter' => '/t',
            ]);
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('invalidCsv', $e->getStringCode());
        }

        $fileId = $this->_client->uploadFile(__DIR__ . '/../../_data/languages.csv', (new \Keboola\StorageApi\Options\FileUploadOptions())->setFileName('test.csv'));
        try {
            $this->_client->apiPost("buckets/{$this->getTestBucketId(self::STAGE_IN)}/tables-async", [
                'dataFileId' => $fileId,
                'delimiter' => '/t',
            ]);
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('invalidCsv', $e->getStringCode());
        }

        $tableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'languages', new CsvFile(__DIR__ . '/../../_data/languages.csv'));
        try {
            $this->_client->apiPost("tables/{$tableId}/import", [
                'dataString' => 'id,name',
                'delimiter' => '/t',
            ]);
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('invalidCsv', $e->getStringCode());
        }

        try {
            $this->_client->apiPost("tables/{$tableId}/import-async", [
                'dataFileId' => $fileId,
                'delimiter' => '/t',
                'incremental' => true,
            ]);
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('invalidCsv', $e->getStringCode());
        }
    }


    /**
     * @expectedException \Keboola\StorageApi\ClientException
     */
    public function testTableNotExistsImport()
    {
        $importCsvFile = new CsvFile(__DIR__ . '/../../_data/languages.csv');
        $this->_client->writeTable('languages', $importCsvFile);
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

        // compare data
        $this->assertLinesEqualsSorted(file_get_contents($extendedFile), $this->_client->getTableDataPreview($tableId, array(
            'format' => 'rfc',
        )), 'imported data comparsion');
    }


    public function testTableAsyncImportMissingFile()
    {
        $token = $this->_client->verifyToken();
        if (in_array($token['owner']['region'], ['eu-central-1', 'ap-northeast-2'])) {
            $this->markTestSkipped('Form upload is not supported for ' . $token['owner']['region'] . ' region.');
            return;
        }

        if (in_array($token['owner']['defaultBackend'], [self::BACKEND_SNOWFLAKE, self::BACKEND_REDSHIFT])) {
            $this->markTestSkipped('TODO: fix issue on redshift and snflk backend.');
            return;
        }

        $filePath = __DIR__ . '/../../_data/languages.csv';
        $importFile = new CsvFile($filePath);
        $tableId = $this->_client->createTable($this->getTestBucketId(), 'languages', $importFile);

        // prepare file but not upload it
        $file = $this->_client->prepareFileUpload((new \Keboola\StorageApi\Options\FileUploadOptions())->setFileName('languages.csv'));

        try {
            $this->_client->writeTableAsyncDirect($tableId, array(
                'dataFileId' => $file['id'],
            ));
            $this->fail('Exception should be thrown');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.fileNotUploaded', $e->getStringCode());
        }
    }

    public function testImportWithoutHeaders()
    {
        $tableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'languages', new \Keboola\Csv\CsvFile(__DIR__ . '/../../_data/languages-headers.csv'));

        $importedFile = __DIR__ . '/../../_data/languages-without-headers.csv';
        $result = $this->_client->writeTableAsync($tableId, new CsvFile($importedFile), array(
            'withoutHeaders' => true,
        ));
        $table = $this->_client->getTable($tableId);

        $this->assertEmpty($result['warnings']);
        $this->assertEmpty($result['transaction']);
        $this->assertNotEmpty($table['dataSizeBytes']);
        $this->assertNotEmpty($result['totalDataSizeBytes']);
    }

    public function testImportWithColumnsList()
    {
        $headersCsv = new \Keboola\Csv\CsvFile(__DIR__ . '/../../_data/languages-headers.csv');
        $tableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'languages', $headersCsv);

        $importedFile = __DIR__ . '/../../_data/languages-without-headers.csv';
        $result = $this->_client->writeTableAsync($tableId, new CsvFile($importedFile), array(
            'columns' => $headersCsv->getHeader(),
        ));
        $table = $this->_client->getTable($tableId);

        $this->assertEmpty($result['warnings']);
        $this->assertEmpty($result['transaction']);
        $this->assertNotEmpty($table['dataSizeBytes']);
        $this->assertNotEmpty($result['totalDataSizeBytes']);
    }

    public function testTableImportFromString()
    {
        $tableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'languages', new \Keboola\Csv\CsvFile(__DIR__ . '/../../_data/languages-headers.csv'));

        $lines = '"id","name"';
        $lines .= "\n" . '"first","second"' . "\n";
        $this->_client->apiPost("tables/$tableId/import", array(
            'dataString' => $lines,
        ));

        $this->assertEquals($lines, $this->_client->getTableDataPreview($tableId, array(
            'format' => 'rfc',
        )));
    }

    public function testTableInvalidAsyncImport()
    {
        $importFile = new CsvFile(__DIR__ . '/../../_data/languages.csv');
        $tableId = $this->_client->createTable($this->getTestBucketId(), 'languages', $importFile);
        $this->_client->addTableColumn($tableId, 'missing');
        try {
            $this->_client->writeTableAsync($tableId, $importFile);
            $this->fail('Exception should be thrown');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('csvImport.columnsNotMatch', $e->getStringCode());
            $this->assertArrayHasKey('exceptionId', $e->getContextParams());
            $this->assertArrayHasKey('job', $e->getContextParams());
            $job = $e->getContextParams()['job'];
            $this->assertEquals('error', $job['status']);
            $this->assertEquals(['missing'], $job['results']['missingColumns']);
            $this->assertEquals(['id', 'name', 'missing'], $job['results']['expectedColumns']);
        }
    }

    public function testTableImportFromEmptyFileShouldFail()
    {
        $tableId = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv')
        );
        try {
            $this->_client->writeTable($tableId, new CsvFile(__DIR__ . '/../../_data/empty.csv'));
            $this->fail('Table should not be imported');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.tables.validation.noColumns', $e->getStringCode());
        }

        try {
            $fileId = $this->_client->uploadFile(__DIR__ . '/../../_data/empty.csv', (new \Keboola\StorageApi\Options\FileUploadOptions())
                ->setFileName('languages')
                ->setCompress(false));
            $this->_client->writeTableAsyncDirect(
                $tableId,
                [
                    'dataFileId' => $fileId
                ]
            );
            $this->fail('Table should not be imported');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('csvImport.columnsNotMatch', $e->getStringCode());
        }
    }

    public function testTableAsyncExportRepeatedly()
    {
        $importFile = new CsvFile(__DIR__ . '/../../_data/languages.csv');
        $firstRunId = $this->_client->generateRunId();
        $this->_client->setRunId($firstRunId);

        $tableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'languages', $importFile);
        $this->_client->writeTable($tableId, $importFile);

        // First export validation
        $tableExportResult = $this->_client->exportTableAsync($tableId);
        $fileInfo = $this->_client->getFile($tableExportResult["file"]["id"], (new \Keboola\StorageApi\Options\GetFileOptions())->setFederationToken(true));

        $this->assertArrayHasKey('maxAgeDays', $fileInfo);
        $this->assertInternalType('integer', $fileInfo['maxAgeDays']);
        $this->assertEquals(StorageApiTestCase::FILE_SHORT_TERM_EXPIRATION_IN_DAYS, $fileInfo['maxAgeDays']);
        $this->assertArrayHasKey('runId', $fileInfo);
        $this->assertEquals($firstRunId, $fileInfo['runId']);
        $this->assertArrayHasKey('runIds', $fileInfo);
        $this->assertEquals([$firstRunId], $fileInfo['runIds']);

        $this->waitForFile($tableExportResult['file']['id']);

        // Another exports validation (cached)
        $oldFileInfo = $fileInfo;
        $secondRunId = $this->_client->generateRunId();
        $this->_client->setRunId($secondRunId);
        $this->_client->exportTableAsync($tableId);

        $thirdRunId = $this->_client->generateRunId();
        $this->_client->setRunId($thirdRunId);
        $this->_client->exportTableAsync($tableId);

        $fourthRunId = $this->_client->generateRunId();
        $this->_client->setRunId($fourthRunId);
        $tableExportResult = $this->_client->exportTableAsync($tableId);
        $fileInfo = $this->_client->getFile($tableExportResult["file"]["id"], (new \Keboola\StorageApi\Options\GetFileOptions())->setFederationToken(true));

        $this->assertArrayHasKey('runId', $fileInfo);
        $this->assertEquals($firstRunId, $fileInfo['runId']);

        $this->assertArrayHasKey('runIds', $fileInfo);
        $this->assertEquals([$firstRunId, $secondRunId, $thirdRunId, $fourthRunId], $fileInfo['runIds']);

        $this->assertTrue($oldFileInfo["id"] === $fileInfo["id"]);
    }

    public function testRowsCountAndSize()
    {
        $importFileIn = new CsvFile(__DIR__ . '/../../_data/languages.csv');
        $importFileOut = new CsvFile(__DIR__ . '/../../_data/languages.more-rows.csv');

        // create tables with same name in different buckets (schemas)
        $inTableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'languages', $importFileIn);
        $outTableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_OUT), 'languages', $importFileOut);

        $inTable = $this->_client->getTable($inTableId);
        $outTable = $this->_client->getTable($outTableId);

        $this->assertEquals(5, $inTable['rowsCount']);
        $this->assertEquals(7, $outTable['rowsCount']);
    }
}
