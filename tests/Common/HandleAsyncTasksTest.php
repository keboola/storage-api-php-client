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

    public function testWriteTableAsyncSuccess()
    {
        $job1 = $this->_client->writeTableAsync('in.c-test.table1', new CsvFile(__DIR__ . '/../_data/languages.csv'), [], false);
        $job2 = $this->_client->writeTableAsync('in.c-test.table2', new CsvFile(__DIR__ . '/../_data/languages.csv'), [], false);
        $results = $this->_client->handleAsyncTasks([$job1, $job2]);
        $this->assertCount(2, $results);
        $table1 = $this->_client->getTable("in.c-test.table1");
        $table2 = $this->_client->getTable("in.c-test.table2");
        $this->assertEquals(5, $table1["rowsCount"]);
        $this->assertEquals(5, $table2["rowsCount"]);
    }

    public function testWriteTableAsyncError()
    {
        $job1 = $this->_client->writeTableAsync('in.c-test.table1', new CsvFile(__DIR__ . '/../_data/languages.csv'), [], false);
        $job2 = $this->_client->writeTableAsync('in.c-test.table2', new CsvFile(__DIR__ . '/../_data/languages.invalid-data.csv'), [], false);
        try {
            $this->_client->handleAsyncTasks([$job1, $job2]);
            $this->fail('Missing exception');
        } catch (ClientException $e) {
            $this->assertContains('invalidData', $e->getStringCode());
        }
        $table1 = $this->_client->getTable("in.c-test.table1");
        $table2 = $this->_client->getTable("in.c-test.table2");
        $this->assertEquals(5, $table1["rowsCount"]);
        $this->assertEquals(0, $table2["rowsCount"]);
    }
}
