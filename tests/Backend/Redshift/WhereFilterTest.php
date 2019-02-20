<?php

declare(strict_types=1);

namespace Keboola\Test\Backend\Redshift;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\ClientException;
use Keboola\Test\StorageApiTestCase;

class WhereFilterTest extends StorageApiTestCase
{
    public function setUp()
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();
    }

    /**
     * @dataProvider conditionProvider
     */
    public function testForbiddenWhereOperators(array $where)
    {
        $csvFile = new CsvFile(tempnam(sys_get_temp_dir(), 'keboola'));
        $csvFile->writeRow(['test']);
        $tableId = $this->_client->createTable($this->getTestBucketId(), 'conditions', $csvFile);

        $this->expectException(ClientException::class);
        $this->expectExceptionMessageRegExp('~Operator ' . $where['operator'] . ' not allowed .* Available operators are~');
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
