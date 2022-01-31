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
use Keboola\StorageApi\TableExporter;
use Keboola\Test\StorageApiTestCase;
use Keboola\StorageApi\Client;
use Keboola\Csv\CsvFile;

class DataPreviewLimitsTest extends StorageApiTestCase
{

    public function setUp()
    {
        parent::setUp();
        $this->initEmptyTestBucketsForParallelTests();
    }

    public function testSimplePreview()
    {
        $tokenData = $this->_client->verifyToken();
        if ($tokenData['owner']['defaultBackend'] === self::BACKEND_REDSHIFT) {
            $this->markTestSkipped('Redshift backend doesn\'t support order in preview.');
        }
        $csv = new CsvFile(tempnam(sys_get_temp_dir(), 'keboola'));
        $csv->writeRow(['Name', 'Id']);
        $csv->writeRow(['aabb', 'test']);
        $csv->writeRow(['ccdd', 'test2']);
        $tableId = $this->_client->createTable($this->getTestBucketId(), 'test1', $csv);

        $data = (array) $this->_client->getTableDataPreview(
            $tableId,
            [
                'format' => 'json',
                'orderBy' => [['column' => 'Id']],
            ]
        );
        // check columns order
        $this->assertEquals('Name', $data['columns'][0]);
        $this->assertEquals('Id', $data['columns'][1]);

        /**
         * @var array<array{value:string,columnName:string,isTruncated:bool}> $row
         */
        foreach ($data['rows'] as $row) {
            $this->assertEquals('Name', $row[0]['columnName']);
            $this->assertEquals('Id', $row[1]['columnName']);
        }
        // check rows
        $this->assertEquals('aabb', $data['rows'][0][0]['value']);
        $this->assertEquals('test', $data['rows'][0][1]['value']);

        $this->assertEquals('ccdd', $data['rows'][1][0]['value']);
        $this->assertEquals('test2', $data['rows'][1][1]['value']);
    }

    public function testDataPreviewDefaultLimit()
    {
        $csvFile = $this->generateCsv(2000);
        $tableId = $this->_client->createTable($this->getTestBucketId(), 'users', $csvFile);

        $preview = $this->_client->getTableDataPreview($tableId);
        $this->assertCount(100, Client::parseCsv($preview), 'only preview of 100 rows should be returned');

        $tableExporter = new TableExporter($this->_client);

        $fullTableExportPath = $this->getExportFilePathForTest('users.csv');
        $tableExporter->exportTable($tableId, $fullTableExportPath, []);
        $this->assertCount(2000, Client::parseCsv(file_get_contents($fullTableExportPath)));
    }

    public function testDataPreviewParametrizedLimit()
    {
        $csvFile = $this->generateCsv(2000);
        $tableId = $this->_client->createTable($this->getTestBucketId(), 'users', $csvFile);

        $preview = $this->_client->getTableDataPreview($tableId, [
            'limit' => 2,
        ]);
        $this->assertCount(2, Client::parseCsv($preview), 'only preview of 2 rows should be returned');
    }

    public function testDataPreviewMaximumLimit()
    {
        $csvFile = $this->generateCsv(2000);
        $tableId = $this->_client->createTable($this->getTestBucketId(), 'users', $csvFile);

        try {
            $this->_client->getTableDataPreview($tableId, [
                'limit' => 1200,
            ]);
            $this->fail('limit 1200 should not be allowed');
        } catch (ClientException $e) {
            $this->assertEquals(400, $e->getCode());
            $this->assertEquals('storage.tables.validation', $e->getStringCode());
            $this->assertContains('1000', $e->getMessage());
        }
    }


    public function testJsonTruncationLimit()
    {
        $tokenData = $this->_client->verifyToken();
        if ($tokenData['owner']['defaultBackend'] === self::BACKEND_SYNAPSE) {
            $this->markTestSkipped('Columns with large length for Synapse backend is not supported yet');
        }

        $columnCount = 5;
        $rowCount = 5;
        $csvFile = $this->generateCsv($rowCount - 1, $columnCount);
        $row = [];
        for ($i = 0; $i < $columnCount; $i++) {
            $row[] = $this->createRandomString(20000);
        }
        $csvFile->writeRow($row);
        $tableId = $this->_client->createTable($this->getTestBucketId(), 'slim', $csvFile);

        $jsonPreview = $this->_client->getTableDataPreview($tableId, ['format' => 'json']);

        $this->assertSame($columnCount, count($jsonPreview['columns']));
        $this->assertSame($rowCount, count($jsonPreview['rows']));
        $this->assertContains('col' . ($columnCount - 1), $jsonPreview['columns']);
        $truncatedRow = $this->getTruncatedRow($jsonPreview);
        $this->assertNotEmpty($truncatedRow);
        $this->assertEquals(16384, mb_strlen($truncatedRow[0]['value']), 'Value in row is not truncated');
    }

    private function getTruncatedRow(array $jsonPreview)
    {
        foreach ($jsonPreview['rows'] as $row) {
            if ($row[0]['isTruncated']) {
                return $row;
            }
        }
        return [];
    }


    private function generateCsv($rowsCount, $collsCount = 2)
    {
        $importFilePath = tempnam(sys_get_temp_dir(), 'keboola');
        $csvFile = new CsvFile($importFilePath);
        $header = [];
        for ($i = 0; $i < $collsCount; $i++) {
            array_push($header, 'col' . $i);
        }
        $csvFile->writeRow($header);
        for ($i = 0; $i < $rowsCount; $i++) {
            $row = [];
            for ($j = 0; $j < $collsCount; $j++) {
                array_push($row, rand());
            }
            $csvFile->writeRow($row);
        }
        return $csvFile;
    }

    /**
     * @param int $length
     * @return string
     */
    private function createRandomString($length)
    {
        $alpabet = "abcdefghijklmnopqrstvuwxyz0123456789 ";
        $randStr = "";
        for ($i = 0; $i < $length; $i++) {
            $randStr .=  $alpabet[rand(0, strlen($alpabet)-1)];
        }
        return $randStr;
    }
}
