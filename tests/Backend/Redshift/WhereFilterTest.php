<?php



namespace Keboola\Test\Backend\Redshift;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\ClientException;
use Keboola\Test\StorageApiTestCase;

class WhereFilterTest extends StorageApiTestCase
{
    public function setUp(): void
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();
    }

    /**
     * @dataProvider conditionProvider
     */
    public function testForbiddenWhereOperators(array $where): void
    {
        $csvFile = $this->createTempCsv();
        $csvFile->writeRow(['test']);
        $tableId = $this->_client->createTableAsync($this->getTestBucketId(), 'conditions', $csvFile);

        $this->expectException(ClientException::class);
        $this->expectExceptionMessageMatches('~Operator ' . $where['operator'] . ' not allowed .* Available operators are~');
        //@phpstan-ignore-next-line
        $this->_client->getTableDataPreview($tableId, ['whereFilters' => [$where]]);
    }

    public function conditionProvider()
    {
        return [
            [
                [
                    'column' => 'test',
                    'operator' => 'gt',
                    'values' => [1],
                ],
            ],
            [
                [
                    'column' => 'test',
                    'operator' => 'lt',
                    'values' => [1],
                ],
            ],
            [
                [
                    'column' => 'test',
                    'operator' => 'ge',
                    'values' => [1],
                ],
            ],
            [
                [
                    'column' => 'test',
                    'operator' => 'le',
                    'values' => [1],
                ],
            ],
        ];
    }
}
