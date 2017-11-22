<?php

namespace Keboola\Test\Backend\Snowflake;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\TableExporter;
use Keboola\Test\StorageApiTestCase;

class TimeTravelTest extends StorageApiTestCase
{
    const dataRetentionLimit = 'storage.dataRetentionTimeInDays';

    private $downloadPath;

    /**
     * @var Client to project that has limit set for `storage.dataRetentionTimeInDays`
     */
    protected $timeTravelClient;

    public function setUp()
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();
        $this->downloadPath = __DIR__ . '/../../_tmp/';
        $this->timeTravelClient = new Client([
            'token' => STORAGE_API_TIMETRAVEL_TOKEN,
            'url' => STORAGE_API_URL,
            'backoffMaxTries' => 1
        ]);
    }

    public function testSetDataRetentionPeriodNotEnabled()
    {
        // initial setting for dataRetentionTimeInDays should be the same as project limit
        // the normal _client does not have time travel enabled so it should error
        $token = $this->_client->verifyToken();
        $this->assertNotContains('storage.dataRetentionTimeInDays', array_keys($token['owner']['limits']));

        // newly created table for client without this limit should have this attribute set to 0
        $tableId = $this->_client->createTable(
            $this->getTestBucketId(),
            'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv')
        );

        $table = $this->_client->getTable($tableId);
        $this->assertEquals(0, $table['dataRetentionTimeInDays']);

        // verify that changing this value should result in an error
        try {
            $this->_client->setTableDataRetentionPeriod($tableId, 5);
            $this->fail('The project does not have time travel set');
        } catch (ClientException $exception) {
            $this->assertEquals('storage.timeTravelNotEnabled', $exception->getStringCode());
        }
    }

    public function testSetDataRetentionPeriod()
    {
        $token = $this->timeTravelClient->verifyToken();

        $timeTravelLimit = $token['owner']['limits']['storage.dataRetentionTimeInDays']['value'];

        // newly created table for client without this limit should have this attribute set to 0
        $tableId = $this->timeTravelClient->createTable(
            $this->getTestBucketId(),
            'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv')
        );

        $table = $this->timeTravelClient->getTable($tableId);
        $this->assertEquals($timeTravelLimit, $table['dataRetentionTimeInDays']);

        // verify that changing this value above the limit should result in an error
        try {
            $this->timeTravelClient->setTableDataRetentionPeriod($tableId, $timeTravelLimit + 5);
            $this->fail('The retention period cannot be set higher than the project limit');
        } catch (ClientException $exception) {
            $this->assertEquals('storage.timeTravelLimitExceeded', $exception->getStringCode());
        }

        // setting the value to less than the limit should be fine
        $dataRetentionTimeInDays = $timeTravelLimit - 1;
        $job = $this->timeTravelClient->setTableDataRetentionPeriod($tableId, $dataRetentionTimeInDays);
        $this->timeTravelClient->waitForJob($job['id']);
        $table = $this->timeTravelClient->getTable($tableId);
        $this->assertEquals($dataRetentionTimeInDays, $table['dataRetentionTimeInDays']);

        // setting the value to < 0 should throw an error
        try {
            $table = $this->timeTravelClient->setTableDataRetentionPeriod($tableId, -10);
            $this->fail('time travel into the future has not been discovered yet.');
        } catch (ClientException $exception) {
            $this->assertEquals('storage.timeTravelInvalidRetentionPeriod', $exception->getStringCode());
        }
    }

    public function testCreateTableFromTimestamp()
    {
        $importFile = new CsvFile(__DIR__ . '/../../_data/languages.csv');

        $sourceTable = 'languages_' . date('Ymd_His');

        $sourceTableId = $this->timeTravelClient->createTable(
            $this->getTestBucketId(),
            $sourceTable,
            $importFile
        );
        $originalTable = $this->timeTravelClient->getTable($sourceTableId);
        // the timestamp must be at least 1 sec > creation time
        sleep(5);
        $timestamp = date(DATE_ATOM);
        sleep(25);

        $this->timeTravelClient->writeTable($sourceTableId, $importFile, ['incremental' => true]);

        $updatedTable = $this->timeTravelClient->getTable($sourceTableId);
        $newTableName = "new-table-name_" . date('Ymd_His', strtotime($timestamp));

        $replicaTableId = $this->timeTravelClient->createTableFromSourceTableAtTimestamp(
            $this->getTestBucketId(self::STAGE_OUT),
            $sourceTableId,
            $timestamp,
            $newTableName
        );

        $replicaTable = $this->timeTravelClient->getTable($replicaTableId);

        $this->assertEquals($newTableName, $replicaTable['name']);

        // Pending fix of timetravel metadata issue case 00022189
        //
        // $this->assertEquals($updatedTable['rowsCount'], $replicaTable['rowsCount'] * 2);
        // $this->assertEquals($originalTable['rowsCount'], $replicaTable['rowsCount']);

        // test data export
        $exporter = new TableExporter($this->timeTravelClient);
        $downloadFile = $this->downloadPath . 'timeTravelOutput.csv';
        $exporter->exportTable($replicaTableId, $downloadFile, []);
        $this->assertArrayEqualsSorted(
            Client::parseCsv(file_get_contents($importFile)),
            Client::parseCsv(file_get_contents($downloadFile)),
            'id'
        );
    }

    public function testCreateafdfdsTableFromTimestampOfAlteredTable()
    {
        $importFile = new CsvFile(__DIR__ . '/../../_data/languages.csv');

        $sourceTable = 'languages_' . date('Ymd_His');

        $sourceTableId = $this->timeTravelClient->createTable(
            $this->getTestBucketId(),
            $sourceTable,
            $importFile
        );
        $originalTable = $this->timeTravelClient->getTable($sourceTableId);
        // the timestamp must be at least 1 sec > creation time
        sleep(5);
        $timestamp = date(DATE_ATOM);
        sleep(25);

        $this->_client->addTableColumn($sourceTableId, "new-column");

        $updatedTable = $this->timeTravelClient->getTable($sourceTableId);

        $newTableName = "new-table-name_" . date('Ymd_His', strtotime($timestamp));

        $replicaTableId = $this->timeTravelClient->createTableFromSourceTableAtTimestamp(
            $this->getTestBucketId(self::STAGE_OUT),
            $sourceTableId,
            $timestamp,
            $newTableName
        );

        $replicaTable = $this->timeTravelClient->getTable($replicaTableId);

        $this->assertEquals($newTableName, $replicaTable['name']);
        $this->assertEquals($originalTable['columns'], $replicaTable['columns']);
        $this->assertGreaterThan(count($replicaTable['columns']), count($updatedTable['columns']));
    }
}
