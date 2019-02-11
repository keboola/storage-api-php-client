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

    public function testForbiddenWhereOperators()
    {
        $csvFile = new CsvFile(tempnam(sys_get_temp_dir(), 'keboola'));
        $csvFile->writeRow(['test']);
        $tableId = $this->_client->createTable($this->getTestBucketId(), 'conditions', $csvFile);

        $where = [
            [
                'column' => 'test',
                'operator' => 'gt',
                'values' => [1]
            ]
        ];

        $this->expectException(ClientException::class);
        $this->expectExceptionMessageRegExp('~Operator gt not allowed .* Available operators are~');
        $this->_client->getTableDataPreview($tableId, ['whereFilters' => $where]);
    }
}
