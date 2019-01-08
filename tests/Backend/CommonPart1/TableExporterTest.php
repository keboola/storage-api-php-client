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
use Keboola\StorageApi\Exception;
use Keboola\Test\StorageApiTestCase;
use Keboola\StorageApi\Client;
use Keboola\Csv\CsvFile;
use Keboola\StorageApi\TableExporter;

class TableExporterTest extends StorageApiTestCase
{


    private $downloadPath;
    private $downloadPathGZip;

    public function setUp()
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();
        $this->downloadPath = __DIR__ . '/../../_tmp/languages.sliced.csv';
        $this->downloadPathGZip = __DIR__ . '/../../_tmp/languages.sliced.csv.gz';
    }

    /**
     * @dataProvider tableImportData
     * @param $importFileName
     */
    public function testTableAsyncExport(array $supportedBackends, CsvFile $importFile, $expectationsFileName, $exportOptions = array())
    {
        $expectationsFile = __DIR__ . '/../../_data/' . $expectationsFileName;
        $tokenData = $this->_client->verifyToken();
        if (!in_array($tokenData['owner']['defaultBackend'], $supportedBackends)) {
            return;
        }

        if (!isset($exportOptions['gzip'])) {
            $exportOptions['gzip'] = false;
        }

        $tableId = $this->_client->createTableAsync($this->getTestBucketId(self::STAGE_IN), 'languages', $importFile);
        $exporter = new TableExporter($this->_client);

        if ($exportOptions['gzip'] === true) {
            $exporter->exportTable($tableId, $this->downloadPathGZip, $exportOptions);
            if (file_exists($this->downloadPath)) {
                unlink($this->downloadPath);
            }
            $process = new \Symfony\Component\Process\Process("gunzip " . escapeshellarg($this->downloadPathGZip));
            if (0 !== $process->run()) {
                throw new \Symfony\Component\Process\Exception\ProcessFailedException($process);
            }
        } else {
            $exporter->exportTable($tableId, $this->downloadPath, $exportOptions);
        }


        // compare data
        $this->assertTrue(file_exists($this->downloadPath));
        $this->assertLinesEqualsSorted(file_get_contents($expectationsFile), file_get_contents($this->downloadPath), 'imported data comparison');
    }

    public function testLimitParameter()
    {
        $importFile = new CsvFile(__DIR__ . '/../../_data/languages.csv');
        $tableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'languages', $importFile);
        $this->_client->writeTable($tableId, $importFile);

        $exportOptions = array(
            'limit' => 2,
        );
        $exporter = new TableExporter($this->_client);
        $exporter->exportTable($tableId, $this->downloadPath, $exportOptions);
        $this->assertTrue(file_exists($this->downloadPath));
        $parsed = Client::parseCsv(file_get_contents($this->downloadPath));
        $this->assertCount($exportOptions['limit'], $parsed);
    }


    public function testExportTables()
    {
        $filesBasePath = __DIR__ . '/../../_data/';
        $table1Id = $this->_client->createTableAsync($this->getTestBucketId(self::STAGE_IN), 'languages1', new CsvFile($filesBasePath . 'languages.csv'));
        $table2Id = $this->_client->createTableAsync($this->getTestBucketId(self::STAGE_IN), 'languages2', new CsvFile($filesBasePath . 'languages.csv'));
        $exporter = new TableExporter($this->_client);
        $file1 = __DIR__ . '/../../_tmp/languages1.csv';
        $file2 = __DIR__ . '/../../_tmp/languages2.csv';
        $exports = array(
            array(
                'tableId' => $table1Id,
                'destination' => $file1
            ),
            array(
                'tableId' => $table2Id,
                'destination' => $file2
            )

        );
        $exporter->exportTables($exports);
        // compare data
        $this->assertTrue(file_exists($file1));
        $this->assertTrue(file_exists($file2));
        $this->assertLinesEqualsSorted(file_get_contents($filesBasePath . 'languages.csv'), file_get_contents($file1), 'imported data comparison');
        $this->assertLinesEqualsSorted(file_get_contents($filesBasePath . 'languages.csv'), file_get_contents($file2), 'imported data comparison');
    }

    public function testExportTablesMissingTableId()
    {
        $exporter = new TableExporter($this->_client);
        try {
            $exporter->exportTables(array(array()));
            $this->fail('Missing exception');
        } catch (Exception $e) {
            $this->assertEquals('Missing tableId', $e->getMessage());
        }
    }

    public function testExportTablesEmptyTableId()
    {
        $exporter = new TableExporter($this->_client);
        try {
            $exporter->exportTables(array(array('tableId' => '')));
            $this->fail('Missing exception');
        } catch (Exception $e) {
            $this->assertEquals('Missing tableId', $e->getMessage());
        }
    }

    public function testExportTablesMissingDestination()
    {
        $exporter = new TableExporter($this->_client);
        try {
            $exporter->exportTables(array(array('tableId' => 'dummy')));
            $this->fail('Missing exception');
        } catch (Exception $e) {
            $this->assertEquals('Missing destination', $e->getMessage());
        }
    }

    public function testExportTablesEmptyDestination()
    {
        $exporter = new TableExporter($this->_client);
        try {
            $exporter->exportTables(array(array('tableId' => 'dummy', 'destination' => '')));
            $this->fail('Missing exception');
        } catch (Exception $e) {
            $this->assertEquals('Missing destination', $e->getMessage());
        }
    }

    public function testExportTablesInvalidTable()
    {
        $exporter = new TableExporter($this->_client);
        try {
            $exporter->exportTables(array(array('tableId' => 'in.c-dummy.dummy', 'destination' => 'dummy')));
            $this->fail('Missing exception');
        } catch (ClientException $e) {
            $this->assertContains('The table "dummy" was not found in the bucket "in.c-dummy"', $e->getMessage());
        }
    }

    public function tableImportData()
    {
        $filesBasePath = __DIR__ . '/../../_data/';
        return array(
            array([self::BACKEND_SNOWFLAKE], new CsvFile($filesBasePath . '1200.csv'), '1200.csv'),
            array([self::BACKEND_REDSHIFT, self::BACKEND_SNOWFLAKE], new CsvFile($filesBasePath . 'languages.csv.gz'), 'languages.csv'),
            array([self::BACKEND_REDSHIFT, self::BACKEND_SNOWFLAKE], new CsvFile($filesBasePath . 'languages.encoding.csv'), 'languages.encoding.csv'),
            array([self::BACKEND_REDSHIFT, self::BACKEND_SNOWFLAKE], new CsvFile($filesBasePath . 'languages.csv.gz'), 'languages.csv', array('gzip' => true)),
            array([self::BACKEND_REDSHIFT, self::BACKEND_SNOWFLAKE], new CsvFile($filesBasePath . 'numbers.csv'), 'numbers.csv'),
            array([self::BACKEND_REDSHIFT, self::BACKEND_SNOWFLAKE], new CsvFile($filesBasePath . 'numbers.csv'), 'numbers.two-cols.csv', array('columns' => array('0', '45'))),

            // tests the redshift data too long bug https://github.com/keboola/connection/issues/412
            array([self::BACKEND_REDSHIFT, self::BACKEND_SNOWFLAKE], new CsvFile($filesBasePath . 'languages.64k.csv'), 'languages.64k.csv'),
            array([self::BACKEND_REDSHIFT, self::BACKEND_SNOWFLAKE], new CsvFile($filesBasePath . 'languages.64k.csv'), 'languages.64k.csv',  array('gzip' => true)),
            array([self::BACKEND_REDSHIFT, self::BACKEND_SNOWFLAKE], new CsvFile($filesBasePath . '64K.csv'), '64K.csv'),
            array([self::BACKEND_REDSHIFT, self::BACKEND_SNOWFLAKE], new CsvFile($filesBasePath . '64K.csv'), '64K.csv',  array('gzip' => true)),

            array([self::BACKEND_REDSHIFT], new CsvFile($filesBasePath . 'escaping.csv'), 'escaping.standard.out.csv', array('gzip' => true)),
            array([self::BACKEND_REDSHIFT], new CsvFile($filesBasePath . 'numbers.csv'), 'numbers.csv', array('gzip' => true)),
            array([self::BACKEND_REDSHIFT], new CsvFile($filesBasePath . 'numbers.csv'), 'numbers.two-cols.csv', array('gzip' => true, 'columns' => array('0', '45'))),
        );
    }
}
