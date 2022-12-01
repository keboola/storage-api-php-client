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
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\StorageApi\Options\GetFileOptions;
use Keboola\Test\StorageApiTestCase;
use Keboola\Csv\CsvFile;

class ImportExportCommonTest extends StorageApiTestCase
{

    public function setUp(): void
    {
        parent::setUp();
        $this->initEmptyTestBucketsForParallelTests();
    }

    /**
     * @dataProvider tableImportData
     */
    public function testTableImportExport(CsvFile $importFile, $expectationsFileName, $colNames, $format = 'rfc', $createTableOptions = []): void
    {
        $this->skipTestForBackend([self::BACKEND_BIGQUERY], 'Don\'t test sync import.');
        $expectationsFile = __DIR__ . '/../../_data/' . $expectationsFileName;
        $tableId = $this->_client->createTable($this->getTestBucketId(), 'languages-2', $importFile, $createTableOptions);

        /** @var array $resultFullLoad */
        $resultFullLoad = $this->_client->writeTable($tableId, $importFile);
        $table = $this->_client->getTable($tableId);

        $this->assertEmpty($resultFullLoad['warnings']);
        $this->assertEquals($colNames, array_values((array) $resultFullLoad['importedColumns']), 'columns');
        $this->assertEmpty($resultFullLoad['transaction']);
        $this->assertNotEmpty($table['dataSizeBytes']);
        $this->assertNotEmpty($resultFullLoad['totalDataSizeBytes']);

        // compare data
        $data1 = $this->_client->getTableDataPreview($tableId, [
            'format' => $format,
        ]);
        $expectedData = file_get_contents($expectationsFile);

        $this->assertLinesEqualsSorted($expectedData, $data1, 'imported data comparsion');

        // incremental
        /** @var array $resultAfterIncremental */
        $resultAfterIncremental = $this->_client->writeTable($tableId, $importFile, [
            'incremental' => true,
        ]);
        $data2 = $this->_client->getTableDataPreview($tableId, [
            'format' => $format,
        ]);

        $expectedNumberOfRows = isset($createTableOptions['primaryKey']) ? $resultFullLoad['totalRowsCount'] : $resultFullLoad['totalRowsCount'] * 2;
        if (isset($createTableOptions['primaryKey'])) {
            $this->assertLinesEqualsSorted($data1, $data2);
        } else {
            $this->assertNotEquals($data1, $data2);
        }
        $this->assertEquals($expectedNumberOfRows, $resultAfterIncremental['totalRowsCount']);
    }

    /**
     * @dataProvider tableImportData
     */
    public function testTableAsyncImportExport(
        CsvFile $importFile,
        string $expectationsFileName,
        array $colNames,
        string $format = 'rfc',
        array $createTableOptions = []
    ): void {
        $expectationsFile = __DIR__ . '/../../_data/' . $expectationsFileName;
        $tableId = $this->_client->createTable($this->getTestBucketId(), 'languages-3', $importFile, $createTableOptions);

        $result = $this->_client->writeTableAsync($tableId, $importFile);
        $table = $this->_client->getTable($tableId);

        $this->assertEmpty($result['warnings']);
        $this->assertEquals($colNames, array_values($result['importedColumns']), 'columns');
        $this->assertEmpty($result['transaction']);
        $this->assertNotEmpty($table['dataSizeBytes']);
        $this->assertNotEmpty($result['totalDataSizeBytes']);

        // compare data
        $this->assertLinesEqualsSorted(file_get_contents($expectationsFile), $this->_client->getTableDataPreview($tableId, [
            'format' => $format,
        ]), 'imported data comparsion');

        // incremental

        $result = $this->_client->writeTableAsync($tableId, $importFile, [
            'incremental' => true,
        ]);
        $this->assertNotEmpty($result['totalDataSizeBytes']);
    }


    public function tableImportData(): array
    {
        return [
            'simple' =>
                [
                    new CsvFile(__DIR__ . '/../../_data/languages.csv'),
                    'languages.csv',
                    ['id', 'name'],
                ],
            'special chars' =>
                [
                    new CsvFile(__DIR__ . '/../../_data/languages.encoding.csv'),
                    'languages.encoding.csv',
                    ['id', 'name'],
                ],
            'duplicates' =>
                [
                    new CsvFile(__DIR__ . '/../../_data/languages.with-duplicates.csv'),
                    'languages.csv',
                    ['id', 'name'],
                    'rfc',
                    [
                        'primaryKey' => 'id,name',
                    ],
                ],
            'special-column-names' =>
                [
                    new CsvFile(__DIR__ . '/../../_data/languages.special-column-names.csv'),
                    'languages.special-column-names.csv',
                    ['Id', 'queryId'],
                    'rfc',
                    [
                        'primaryKey' => 'Id,queryId',
                    ],
                ],
            'utf8.bom' =>
                [
                    new CsvFile(__DIR__ . '/../../_data/languages.utf8.bom.csv'),
                    'languages.csv',
                    ['id', 'name'],
                ],
            'gz' =>
                [
                    new CsvFile(__DIR__ . '/../../_data/languages.csv.gz'),
                    'languages.csv',
                    ['id', 'name'],
                ],
            'escaping' =>
                [
                    new CsvFile(__DIR__ . '/../../_data/escaping.csv'),
                    'escaping.standard.out.csv',
                    ['col1', 'col2_with_space'],
                ],
            'nl on last row' =>
                [
                    new CsvFile(__DIR__ . '/../../_data/escaping.nl-last-row.csv'),
                    'escaping.standard.out.csv',
                    ['col1', 'col2_with_space'],
                ],

        ];
    }

    /**
     * @dataProvider incrementalImportPkDedupeData
     * @param $createFile
     * @param $primaryKey
     * @param $expectationFileAfterCreate
     * @param $incrementFile
     * @param $expectationFinal
     */
    public function testIncrementalImportPkDedupe($createFile, $primaryKey, $expectationFileAfterCreate, $incrementFile, $expectationFinal): void
    {

        $tableId = $this->_client->createTableAsync($this->getTestBucketId(), 'pk', $createFile, [
            'primaryKey' => $primaryKey,
        ]);

        $this->assertLinesEqualsSorted(file_get_contents($expectationFileAfterCreate), $this->_client->getTableDataPreview($tableId));

        $this->_client->writeTableAsync($tableId, $incrementFile, [
            'incremental' => true,
        ]);
        $this->assertLinesEqualsSorted(file_get_contents($expectationFinal), $this->_client->getTableDataPreview($tableId));
    }

    public function incrementalImportPkDedupeData(): array
    {
        return [
            'simple' =>
                [
                    new CsvFile(__DIR__ . '/../../_data/pk.simple.csv'),
                    'id',
                    new CsvFile(__DIR__ . '/../../_data/pk.simple.loaded.csv'),
                    new CsvFile(__DIR__ . '/../../_data/pk.simple.increment.csv'),
                    new CsvFile(__DIR__ . '/../../_data/pk.simple.increment.loaded.csv'),
                ],
            'multiple' =>
                [
                    new CsvFile(__DIR__ . '/../../_data/pk.multiple.csv'),
                    'id,sub_id',
                    new CsvFile(__DIR__ . '/../../_data/pk.multiple.loaded.csv'),
                    new CsvFile(__DIR__ . '/../../_data/pk.multiple.increment.csv'),
                    new CsvFile(__DIR__ . '/../../_data/pk.multiple.increment.loaded.csv'),
                ],
        ];
    }

    public function testTableImportColumnsCaseInsensitive(): void
    {
        $tokenData = $this->_client->verifyToken();
        if ($tokenData['owner']['defaultBackend'] !== self::BACKEND_REDSHIFT) {
            self::markTestSkipped('test available for RS only');
        }

        $importFile = new CsvFile(__DIR__ . '/../../_data/languages.csv');
        $tableId = $this->_client->createTable($this->getTestBucketId(), 'languages', $importFile);

        $this->_client->writeTableAsync($tableId, new CsvFile(__DIR__ . '/../../_data/languages.camel-case-columns.csv'));

        $table = $this->_client->getTable($tableId);
        $this->assertEquals($importFile->getHeader(), $table['columns']);
    }

    public function testTableImportCaseSensitiveThrowsUserError(): void
    {
        $tokenData = $this->_client->verifyToken();
        if ($tokenData['owner']['defaultBackend'] !== self::BACKEND_SNOWFLAKE) {
            self::markTestSkipped('Test case-sensitivity columns name only for snowflake');
        }

        $importFile = new CsvFile(__DIR__ . '/../../_data/languages.csv');
        $tableId = $this->_client->createTable($this->getTestBucketId(), 'languages-case-sensitive', $importFile);

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage(
            'Some columns are missing in the csv file. Missing columns: id,name. '
            . 'Expected columns: id,name. Please check if the expected delimiter "," is used in the csv file.'
        );

        $this->_client->writeTableAsync($tableId, new CsvFile(__DIR__ . '/../../_data/languages.camel-case-columns.csv'), ['incremental' => true]);
    }

    /**
     * @dataProvider tableImportInvalidData
     */
    public function testTableInvalidImport($languagesFile): void
    {
        $this->expectException(ClientException::class);
        $importCsvFile = new CsvFile(__DIR__ . '/../../_data/' . $languagesFile);
        $tableId = $this->_client->createTable($this->getTestBucketId(), 'languages', new CsvFile(__DIR__ . '/../../_data/languages.csv'));

        $this->_client->writeTable($tableId, $importCsvFile);
    }

    public function tableImportInvalidData(): array
    {
        return [
            ['languages.invalid.csv'],
            ['languages.invalid.gzip'],
            ['languages.invalid.zip'],
            ['languages.invalid.duplicateColumns.csv'],
        ];
    }

    public function testTableImportNotExistingFile(): void
    {
        try {
            $this->_client->writeTable($this->getTestBucketId() . '.languages', new CsvFile('invalid.csv'));
        } catch (ClientException $e) {
            $this->assertEquals('fileNotReadable', $e->getStringCode());
        }
    }

    public function testTableImportInvalidCsvParams(): void
    {
        try {
            $this->_client->apiPost("buckets/{$this->getTestBucketId()}/tables", [
                'name' => 'invalidTable',
                'dataString' => 'id,name',
                'delimiter' => '/t',
            ]);
        } catch (ClientException $e) {
            $this->assertEquals('invalidCsv', $e->getStringCode());
        }

        $fileId = $this->_client->uploadFile(__DIR__ . '/../../_data/languages.csv', (new FileUploadOptions())->setFileName('test.csv'));
        try {
            $this->_client->apiPost("buckets/{$this->getTestBucketId()}/tables-async", [
                'name' => 'invalidTable',
                'dataFileId' => $fileId,
                'delimiter' => '/t',
            ]);
        } catch (ClientException $e) {
            $this->assertEquals('invalidCsv', $e->getStringCode());
        }

        $tableId = $this->_client->createTable($this->getTestBucketId(), 'languages', new CsvFile(__DIR__ . '/../../_data/languages.csv'));
        try {
            $this->_client->apiPost("tables/{$tableId}/import", [
                'name' => 'invalidTable',
                'dataString' => 'id,name',
                'delimiter' => '/t',
            ]);
        } catch (ClientException $e) {
            $this->assertEquals('invalidCsv', $e->getStringCode());
        }

        try {
            $this->_client->apiPost("tables/{$tableId}/import-async", [
                'name' => 'invalidTable',
                'dataFileId' => $fileId,
                'delimiter' => '/t',
                'incremental' => true,
            ]);
        } catch (ClientException $e) {
            $this->assertEquals('invalidCsv', $e->getStringCode());
        }
    }


    public function testTableNotExistsImport(): void
    {
        $this->expectException(ClientException::class);
        $importCsvFile = new CsvFile(__DIR__ . '/../../_data/languages.csv');
        $this->_client->writeTable('languages', $importCsvFile);
    }

    public function testTableImportCreateMissingColumns(): void
    {
        $this->skipTestForBackend([
            self::BACKEND_BIGQUERY,
            self::BACKEND_TERADATA,
        ], 'Automatic add columns is not implemented.');

        $filePath = __DIR__ . '/../../_data/languages.camel-case-columns.csv';
        $importFile = new CsvFile($filePath);
        $tableId = $this->_client->createTable($this->getTestBucketId(), 'languages', $importFile);

        $extendedFile = __DIR__ . '/../../_data/languages-more-columns.csv';

        /** @var array $result */
        $result = $this->_client->writeTable($tableId, new CsvFile($extendedFile));
        $table = $this->_client->getTable($tableId);

        $this->assertEmpty($result['warnings']);
        $this->assertEquals(['Id', 'Name', 'iso', 'Something'], array_values((array) $result['importedColumns']), 'columns');
        $this->assertEmpty($result['transaction']);
        $this->assertNotEmpty($table['dataSizeBytes']);
        $this->assertNotEmpty($result['totalDataSizeBytes']);

        // compare data
        $this->assertLinesEqualsSorted(file_get_contents($extendedFile), $this->_client->getTableDataPreview($tableId, [
            'format' => 'rfc',
        ]), 'imported data comparsion');
    }


    public function testTableAsyncImportMissingFile(): void
    {
        $token = $this->_client->verifyToken();
        if (in_array($token['owner']['region'], ['eu-central-1', 'ap-northeast-2'])) {
            $this->markTestSkipped('Form upload is not supported for ' . $token['owner']['region'] . ' region.');
        }

        if (in_array($token['owner']['defaultBackend'], [
            self::BACKEND_SNOWFLAKE,
            self::BACKEND_REDSHIFT,
            self::BACKEND_EXASOL,
            self::BACKEND_TERADATA,
        ])) {
            $this->markTestSkipped('TODO: fix issue on redshift and snflk backend.');
        }

        $filePath = __DIR__ . '/../../_data/languages.csv';
        $importFile = new CsvFile($filePath);
        $tableId = $this->_client->createTable($this->getTestBucketId(), 'languages', $importFile);

        // prepare file but not upload it
        $file = $this->_client->prepareFileUpload((new FileUploadOptions())->setFileName('languages.csv'));

        try {
            $this->_client->writeTableAsyncDirect($tableId, [
                'dataFileId' => $file['id'],
            ]);
            $this->fail('Exception should be thrown');
        } catch (ClientException $e) {
            $this->assertEquals('storage.fileNotUploaded', $e->getStringCode());
        }
    }

    public function testImportWithoutHeaders(): void
    {
        $tableId = $this->_client->createTable($this->getTestBucketId(), 'languages', new CsvFile(__DIR__ . '/../../_data/languages-headers.csv'));

        $importedFile = __DIR__ . '/../../_data/languages-without-headers.csv';

        /** @var array $result */
        $result = $this->_client->writeTableAsync($tableId, new CsvFile($importedFile), [
            'withoutHeaders' => true,
        ]);
        $table = $this->_client->getTable($tableId);

        $this->assertEmpty($result['warnings']);
        $this->assertEmpty($result['transaction']);
        $this->assertNotEmpty($table['dataSizeBytes']);
        $this->assertNotEmpty($result['totalDataSizeBytes']);
    }

    public function testImportWithColumnsList(): void
    {
        $headersCsv = new CsvFile(__DIR__ . '/../../_data/languages-headers.csv');
        $tableId = $this->_client->createTable($this->getTestBucketId(), 'languages', $headersCsv);

        $importedFile = __DIR__ . '/../../_data/languages-without-headers.csv';
        /** @var array $result */
        $result = $this->_client->writeTableAsync($tableId, new CsvFile($importedFile), [
            'columns' => $headersCsv->getHeader(),
        ]);
        $table = $this->_client->getTable($tableId);

        $this->assertEmpty($result['warnings']);
        $this->assertEmpty($result['transaction']);
        $this->assertNotEmpty($table['dataSizeBytes']);
        $this->assertNotEmpty($result['totalDataSizeBytes']);
    }

    public function testTableImportFromString(): void
    {
        $tableId = $this->_client->createTable($this->getTestBucketId(), 'languages', new CsvFile(__DIR__ . '/../../_data/languages-headers.csv'));

        $lines = '"id","name"';
        $lines .= "\n" . '"first","second"' . "\n";
        $this->_client->apiPost("tables/$tableId/import", [
            'dataString' => $lines,
        ]);

        $this->assertEquals($lines, $this->_client->getTableDataPreview($tableId, [
            'format' => 'rfc',
        ]));
    }

    public function testTableInvalidAsyncImport(): void
    {
        $this->skipTestForBackend([
            self::BACKEND_BIGQUERY,
            self::BACKEND_TERADATA,
        ], 'Automatic add columns is not implemented.');

        $importFile = new CsvFile(__DIR__ . '/../../_data/languages.csv');
        $tableId = $this->_client->createTable($this->getTestBucketId(), 'languages', $importFile);
        $this->_client->addTableColumn($tableId, 'missing');
        try {
            $this->_client->writeTableAsync($tableId, $importFile);
            $this->fail('Exception should be thrown');
        } catch (ClientException $e) {
            $this->assertEquals('csvImport.columnsNotMatch', $e->getStringCode());
            $this->assertArrayHasKey('exceptionId', $e->getContextParams());
            $this->assertArrayHasKey('job', $e->getContextParams());
            $job = $e->getContextParams()['job'];
            $this->assertEquals('error', $job['status']);
            $this->assertEquals(['missing'], $job['results']['missingColumns']);
            $this->assertEquals(['id', 'name', 'missing'], $job['results']['expectedColumns']);
        }
    }

    public function testTableImportFromEmptyFileShouldFail(): void
    {
        $tableId = $this->_client->createTable(
            $this->getTestBucketId(),
            'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv')
        );
        try {
            $this->_client->writeTable($tableId, new CsvFile(__DIR__ . '/../../_data/empty.csv'));
            $this->fail('Table should not be imported');
        } catch (ClientException $e) {
            $this->assertEquals('storage.tables.validation.noColumns', $e->getStringCode());
        }

        try {
            $fileId = $this->_client->uploadFile(__DIR__ . '/../../_data/empty.csv', (new FileUploadOptions())
                ->setFileName('languages')
                ->setCompress(false));
            $this->_client->writeTableAsyncDirect(
                $tableId,
                [
                    'dataFileId' => $fileId,
                ]
            );
            $this->fail('Table should not be imported');
        } catch (ClientException $e) {
            $this->assertEquals('csvImport.columnsNotMatch', $e->getStringCode());
        }
    }

    public function testTableAsyncExportRepeatedly(): void
    {
        $importFile = new CsvFile(__DIR__ . '/../../_data/languages.csv');
        $firstRunId = $this->_client->generateRunId();
        $this->_client->setRunId($firstRunId);

        $tableId = $this->_client->createTable($this->getTestBucketId(), 'languages', $importFile);
        $this->_client->writeTable($tableId, $importFile);

        // First export validation
        $tableExportResult = $this->_client->exportTableAsync($tableId);
        $fileInfo = $this->_client->getFile($tableExportResult['file']['id'], (new GetFileOptions())->setFederationToken(true));

        $this->assertArrayHasKey('maxAgeDays', $fileInfo);
        $this->assertIsInt($fileInfo['maxAgeDays']);
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
        $fileInfo = $this->_client->getFile($tableExportResult['file']['id'], (new GetFileOptions())->setFederationToken(true));

        $this->assertArrayHasKey('runId', $fileInfo);
        $this->assertEquals($firstRunId, $fileInfo['runId']);

        $this->assertArrayHasKey('runIds', $fileInfo);
        $this->assertEquals([$firstRunId, $secondRunId, $thirdRunId, $fourthRunId], $fileInfo['runIds']);

        $this->assertTrue($oldFileInfo['id'] === $fileInfo['id']);
    }

    public function testRowsCountAndSize(): void
    {
        $importFileIn = new CsvFile(__DIR__ . '/../../_data/languages.csv');
        $importFileOut = new CsvFile(__DIR__ . '/../../_data/languages.more-rows.csv');

        // create tables with same name in different buckets (schemas)
        $inTableId = $this->_client->createTable($this->getTestBucketId(), 'languages', $importFileIn);
        $outTableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_OUT), 'languages', $importFileOut);

        $inTable = $this->_client->getTable($inTableId);
        $outTable = $this->_client->getTable($outTableId);

        $this->assertEquals(5, $inTable['rowsCount']);
        $this->assertEquals(7, $outTable['rowsCount']);
    }
}
