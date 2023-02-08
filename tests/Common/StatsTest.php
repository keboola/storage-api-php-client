<?php
/**
 *
 * User: Martin Halamíček
 * Date: 16.5.12
 * Time: 11:46
 *
 */

namespace Keboola\Test\Common;

use Keboola\Test\StorageApiTestCase;
use Keboola\Csv\CsvFile;

class StatsTest extends StorageApiTestCase
{

    public function setUp(): void
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();
    }

    public function testGetStatsForDifferentJobWithTheSameNumberBeforeDot(): void
    {
        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);

        $importFile = __DIR__ . '/../_data/languages.csv';
        $table1Id = $this->_client->createTableAsync($this->getTestBucketId(self::STAGE_IN), 'languages', new CsvFile($importFile));
        $this->_client->writeTableAsync($table1Id, new CsvFile($importFile));

        // block until async events are processed, processing in order is not guaranteed but it should work most of time
        $this->createAndWaitForEvent((new \Keboola\StorageApi\Event())->setComponent('dummy')->setMessage('dummy'));

        // stats for other run id with the same number before "." should return empty stats
        $newRunId = $this->_client->generateRunId($runId);
        $stats = $this->_client->getStats((new \Keboola\StorageApi\Options\StatsOptions())->setRunId($newRunId));

        $this->assertArrayHasKey('tables', $stats);

        $tables = $stats['tables'];
        $this->assertArrayHasKey('import', $tables);
        $this->assertArrayHasKey('export', $tables);

        $import = $tables['import'];
        $this->assertCount(0, $import['tables']);
        $this->assertEquals(0, $import['totalCount']);

        $export = $tables['export'];
        $this->assertCount(0, $export['tables']);
        $this->assertEquals(0, $export['totalCount']);

        $this->assertArrayHasKey('files', $stats);
        $files = $stats['files'];
        $this->assertEquals(0, $files['total']['count']);

        $this->assertCount(0, $files['tags']['tags']);
    }

    public function testRunIdStats(): void
    {
        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);

        $importFile = __DIR__ . '/../_data/languages.csv';
        $table1Id = $this->_client->createTableAsync($this->getTestBucketId(self::STAGE_IN), 'languages', new CsvFile($importFile));
        $this->_client->writeTableAsync($table1Id, new CsvFile($importFile));

        $table2Id = $this->_client->createTableAsync($this->getTestBucketId(self::STAGE_IN), 'other', new CsvFile($importFile));

        $this->_client->exportTableAsync($table2Id);

        // block until async events are processed, processing in order is not guaranteed but it should work most of time
        $this->createAndWaitForEvent((new \Keboola\StorageApi\Event())->setComponent('dummy')->setMessage('dummy'));

        $stats = $this->_client->getStats((new \Keboola\StorageApi\Options\StatsOptions())->setRunId($runId));

        $this->assertArrayHasKey('tables', $stats);

        $tables = $stats['tables'];
        $this->assertArrayHasKey('import', $tables);
        $this->assertArrayHasKey('export', $tables);

        $import = $tables['import'];
        $this->assertCount(2, $import['tables']);
        $this->assertEquals(3, $import['totalCount']);

        $table = reset($import['tables']);
        $this->assertArrayHasKey('id', $table);
        $this->assertEquals(2, $table['count']);
        $this->assertArrayHasKey('durationTotalSeconds', $table);
        $this->assertEquals($table1Id, $table['id']);

        $export = $tables['export'];
        $this->assertCount(1, $export['tables']);
        $this->assertEquals(1, $export['totalCount']);

        $this->assertArrayHasKey('files', $stats);
        $files = $stats['files'];
        $this->assertEquals(4, $files['total']['count']); // 3 imports + 1 export

        $this->assertCount(3, $files['tags']['tags']);
    }

    public function testEmptyStatsResults(): void
    {
        $stats = $this->_client->getStats((new \Keboola\StorageApi\Options\StatsOptions())->setRunId($this->_client->generateRunId()));

        $this->assertCount(0, $stats['tables']['import']['tables']);
        $this->assertCount(0, $stats['tables']['export']['tables']);
        $this->assertCount(0, $stats['files']['tags']['tags']);
        $this->assertEquals(0, $stats['files']['total']['count']);
    }

    public function testStatsMissingParam(): void
    {
        try {
            $this->_client->getStats(new \Keboola\StorageApi\Options\StatsOptions());
            $this->fail('should not be allowed get stats without runId');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals(400, $e->getCode());
            $this->assertEquals('storage.validation', $e->getStringCode());
        }
    }
}
