<?php

namespace Keboola\Test\Backend\Teradata;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\TableExporter;
use Keboola\Test\StorageApiTestCase;
use function GuzzleHttp\json_encode;

class OrderByTest extends StorageApiTestCase
{
    public function setUp(): void
    {
        parent::setUp();
        $this->initEmptyTestBucketsForParallelTests();
    }

    public function testSimpleSort(): void
    {
        $tableId = $this->prepareTable();

        $order = [
            'column' => 'column_string',
        ];

        $dataPreview = $this->_client->getTableDataPreview($tableId, ['orderBy' => [$order]]);
        $this->assertSame('aa', Client::parseCsv($dataPreview)[0]['column_string']);
        // TODO enable when export supports filters
//        $exportTable = $this->getExportedTable($tableId, ['orderBy' => [$order]]);
//        $this->assertSame('aa', $exportTable[0]['column_string']);
    }

    public function testSortWithDataType(): void
    {
        $tableId = $this->prepareTable();

        $order = [
            'column' => 'column_double',
            'dataType' => 'REAL',
        ];
        $dataPreview = $this->_client->getTableDataPreview($tableId, ['orderBy' => [$order]]);
        $this->assertSame('1.1234', Client::parseCsv($dataPreview)[0]['column_double']);
        // TODO enable when export supports filters
//        $exportTable = $this->getExportedTable($tableId, ['orderBy' => [$order]]);
//        $this->assertSame('1.1234', $exportTable[0]['column_double']);
    }

    public function testComplexSort(): void
    {
        $tableId = $this->prepareTable();

        $order = [
            [
                'column' => 'column_string',
                'order' => 'DESC',
            ],
            [
                'column' => 'column_string_number',
                'order' => 'ASC',
                'dataType' => 'INTEGER',
            ],
        ];

        $dataPreview = $this->_client->getTableDataPreview($tableId, ['orderBy' => $order]);
        $this->assertSame('5', Client::parseCsv($dataPreview)[0]['column_string_number']);
        // TODO enable when export supports filters
//        $exportTable = $this->getExportedTable($tableId, ['orderBy' => $order]);
//        $this->assertSame('5', $exportTable[0]['column_string_number']);
    }

    /**
     * @param array<string, string> $order
     * @dataProvider invalidDataProvider
     */
    public function testInvalidOrderByParamsShouldReturnErrorInDataPreview(array $order, string $message): void
    {
        $tableId = $this->prepareTable();

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage($message);
        $this->_client->getTableDataPreview($tableId, ['orderBy' => [$order]]);
    }

    /**
     * @param array<string, string> $order
     * @dataProvider invalidDataProvider
     */
    public function testInvalidOrderByParamsShouldReturnErrorInExport(array $order, string $message): void
    {
        // TODO enable when export supports filters
        $this->markTestSkipped('export does not supports filters');
//        $tableId = $this->prepareTable();
//
//        $this->expectException(ClientException::class);
//        $this->expectExceptionMessage($message);
//        $this->getExportedTable($tableId, ['orderBy' => [$order]]);
    }

    /**
     * @return array{0: array<string, string>, 1: string}[]
     */
    public function invalidDataProvider(): array
    {
        return [
            [
                [
                    'column' => 'non-existing',
                ],
                'Can\'t filter by column non-existing, column does not exist',
            ],
            [
                [
                    'column' => 'column_string',
                    'order' => 'non-existing',
                ],
                'Invalid sort order non-existing. Available orders are [DESC|ASC]',
            ],
            [
                [
                    'column' => 'column_string',
                    'order' => 'DESC',
                    'dataType' => 'non-existing',
                ],
                'Data type non-existing not recognized.',
            ],
        ];
    }

    public function testNonArrayParamsShouldReturnErrorInAsyncExport(): void
    {
        // TODO enable when export supports filters
        $this->markTestSkipped('export does not supports filters');
//        $tableId = $this->prepareTable();
//
//        $orderBy = ['column' => 'column'];
//
//        $this->expectException(ClientException::class);
//        $this->expectExceptionMessage("All items in param \"orderBy\" should be an arrays, but parameter contains:\n" . json_encode($orderBy));
//        $this->getExportedTable($tableId, ['orderBy' => $orderBy]);
    }


    public function testInvalidStructuredQueryInAsyncExport(): void
    {
        // TODO enable when export supports filters
        $this->markTestSkipped('export does not supports filters');
//        $tableId = $this->prepareTable();
//
//        $orderBy = 'string';
//
//        $this->expectException(ClientException::class);
//        $this->expectExceptionMessage("Parameter \"orderBy\" should be an array, but parameter contains:\n" . json_encode($orderBy));
//        $this->getExportedTable($tableId, ['orderBy' => $orderBy]);
    }

    public function testNonArrayParamsShouldReturnErrorInDataPreview(): void
    {
        $tableId = $this->prepareTable();

        $orderBy = ['column' => 'column'];

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage("All items in param \"orderBy\" should be an arrays, but parameter contains:\n" . json_encode($orderBy));
        $this->_client->getTableDataPreview($tableId, ['orderBy' => $orderBy]);
    }


    public function testInvalidStructuredQueryInADataPreview(): void
    {
        $tableId = $this->prepareTable();

        $orderBy = 'string';

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage("Parameter \"orderBy\" should be an array, but parameter contains:\n" . json_encode($orderBy));
        $this->_client->getTableDataPreview($tableId, ['orderBy' => $orderBy]);
    }


    private function getExportedTable(string $tableId, array $exportOptions): array
    {
        $tableExporter = new TableExporter($this->_client);
        $path = tempnam(sys_get_temp_dir(), 'keboola-export');
        $this->assertIsString($path);
        $tableExporter->exportTable($tableId, $path, $exportOptions);
        $fileContents = file_get_contents($path);
        $this->assertIsString($fileContents);
        return Client::parseCsv($fileContents);
    }

    private function prepareTable(): string
    {
        $csvFile = new CsvFile(tempnam(sys_get_temp_dir(), 'keboola'));
        $csvFile->writeRow(['column_string', 'column_string_number', 'column_double']);
        $csvFile->writeRow(['ab', '1', '003.123']);
        $csvFile->writeRow(['bc', '4', '3.123']);
        $csvFile->writeRow(['aa', '4444', '0004.123']);
        $csvFile->writeRow(['zx', '5', '4']);
        $csvFile->writeRow(['zx', '555111', '1.1234']);
        $tableId = $this->_client->createTable($this->getTestBucketId(), 'conditions', $csvFile);
        $this->assertIsString($tableId);
        return $tableId;
    }
}
