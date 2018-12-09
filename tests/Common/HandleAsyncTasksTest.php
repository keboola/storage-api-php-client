<?php
namespace Keboola\Test\Common;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\Test\StorageApiTestCase;

class HandleAsyncTasksTest extends StorageApiTestCase
{
    public function setUp()
    {
        parent::setUp();
        $this->_client->createBucket('test', Client::STAGE_IN);
        $this->_client->createTableAsync('in.c-test', 'table1', new CsvFile(__DIR__ . '/../_data/languages-headers.csv'));
        $this->_client->createTableAsync('in.c-test', 'table2', new CsvFile(__DIR__ . '/../_data/languages-headers.csv'));
    }

    public function tearDown()
    {
        parent::tearDown();
        $this->_client->dropBucket('in.c-test', ['force' => true]);
    }

    public function testSuccess()
    {
        $job1Info = $this->_client->writeTableAsync('in.c-test.table1', new CsvFile(__DIR__ . '/../_data/languages.csv'), [], false);
        $job2Info = $this->_client->writeTableAsync('in.c-test.table2', new CsvFile(__DIR__ . '/../_data/languages.csv'), [], false);
        $results = $this->_client->handleAsyncTasks([$job1Info, $job2Info]);
        $this->assertCount(2, $results);
        $table1Info = $this->_client->getTable("in.c-test.table1");
        $table2Info = $this->_client->getTable("in.c-test.table2");
        $this->assertEquals(5, $table1Info["rowsCount"]);
        $this->assertEquals(5, $table2Info["rowsCount"]);
    }

    public function testError()
    {
        $job1Info = $this->_client->writeTableAsync('in.c-test.table1', new CsvFile(__DIR__ . '/../_data/languages.csv'), [], false);
        $job2Info = $this->_client->writeTableAsync('in.c-test.table2', new CsvFile(__DIR__ . '/../_data/languages.invalid-data.csv'), [], false);
        try {
            $this->_client->handleAsyncTasks([$job1Info, $job2Info]);
            $this->fail('Missing exception');
        } catch (ClientException $e) {
            $this->assertContains('invalidData', $e->getStringCode());
        }
        $table1Info = $this->_client->getTable("in.c-test.table1");
        $table2Info = $this->_client->getTable("in.c-test.table2");
        $this->assertEquals(5, $table1Info["rowsCount"]);
        $this->assertEquals(0, $table2Info["rowsCount"]);
    }
}
