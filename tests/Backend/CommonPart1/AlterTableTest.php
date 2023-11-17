<?php

namespace Keboola\Test\Backend\CommonPart1;

use Keboola\StorageApi\ClientException;
use Keboola\Test\StorageApiTestCase;
use Keboola\Csv\CsvFile;

class AlterTableTest extends StorageApiTestCase
{

    public function setUp(): void
    {
        parent::setUp();
        $this->initEmptyTestBucketsForParallelTests();
    }

    public function testTableColumnAdd(): void
    {
        $importFile = __DIR__ . '/../../_data/languages.csv';
        $tableId = $this->_client->createTableAsync($this->getTestBucketId(), 'languages', new CsvFile($importFile));

        $this->_client->addTableColumn($tableId, 'State');

        $detail = $this->_client->getTable($tableId);

        $this->assertArrayHasKey('columns', $detail);
        $this->assertContains('State', $detail['columns']);
        $this->assertEquals(['id', 'name', 'State'], $detail['columns']);

        $importFileWithNewCol = __DIR__ . '/../../_data/languages.with-state.csv';
        $this->_client->writeTableAsync($tableId, new CsvFile($importFileWithNewCol));
        $this->assertLinesEqualsSorted(
            file_get_contents($importFileWithNewCol),
            $this->_client->getTableDataPreview($tableId),
            'new column is imported'
        );
    }

    /**
     * @dataProvider webalizeColumnNameProvider
     */
    public function testTableColumnNameShouldBeWebalized(string $requestedColumnName, string $expectedColumnName): void
    {
        $importFile = __DIR__ . '/../../_data/languages.csv';
        $tableId = $this->_client->createTableAsync($this->getTestBucketId(), 'languages', new CsvFile($importFile));

        $this->_client->addTableColumn($tableId, $requestedColumnName);

        $table = $this->_client->getTable($tableId);
        $this->assertEquals(['id', 'name', $expectedColumnName], $table['columns']);
    }

    public function webalizeColumnNameProvider(): array
    {
        return [
            'dashes + underscores' => [
                '_abc-def----ghi_',
                'abc_def_ghi',
            ],
            'diacritics + spaces' => [
                'žluťoučký    kůň',
                'zlutoucky_kun',
            ],
            'more underscores' => [
                'lot__of_____underscores____',
                'lot__of_____underscores',
            ],
        ];
    }

    public function testTableExistingColumnAdd(): void
    {
        $this->expectException(ClientException::class);
        $importFile = __DIR__ . '/../../_data/languages.csv';
        $tableId = $this->_client->createTableAsync($this->getTestBucketId(), 'languages', new CsvFile($importFile));
        $this->_client->addTableColumn($tableId, 'id');
    }

    public function testsTableExistingColumnAddWithDifferentCaseShouldThrowError(): void
    {
        $importFile = __DIR__ . '/../../_data/languages.csv';
        $tableId = $this->_client->createTableAsync($this->getTestBucketId(), 'languages', new CsvFile($importFile));
        try {
            $this->_client->addTableColumn($tableId, 'ID');
        } catch (ClientException $e) {
            $this->assertEquals('storage.tables.columnAlreadyExists', $e->getStringCode());
        }
    }

    /**
     * @dataProvider invalidColumnNameProvider
     */
    public function testAddColumnWithInvalidNameShouldThrowError(string $columnName): void
    {
        $importFile = __DIR__ . '/../../_data/languages.csv';
        $tableId = $this->_client->createTableAsync($this->getTestBucketId(), 'languages', new CsvFile($importFile));
        try {
            $this->_client->addTableColumn(
                $tableId,
                $columnName
            );
            $this->fail('Column should not be created');
        } catch (ClientException $e) {
            $this->assertEquals('storage.tables.invalidColumnName', $e->getStringCode());
        }
    }

    public function invalidColumnNameProvider(): array
    {
        return [
            'too long column' => [
                str_repeat('x', 65),
            ],
            'empty column' => [
                '',
            ],
        ];
    }

    public function testTableColumnDelete(): void
    {
        $importFile = __DIR__ . '/../../_data/languages.camel-case-columns.csv';
        $tableId = $this->_client->createTableAsync($this->getTestBucketId(), 'languages', new CsvFile($importFile));

        $this->_client->deleteTableColumn($tableId, 'Name');

        $detail = $this->_client->getTable($tableId);
        $this->assertEquals(['Id'], $detail['columns']);

        try {
            $this->_client->deleteTableColumn($tableId, 'Id');
            $this->fail('Exception should be thrown when last column is remaining');
        } catch (ClientException $e) {
        }
    }

    public function testTablePkColumnDelete(): void
    {
        $tokenData = $this->_client->verifyToken();
        if ($tokenData['owner']['defaultBackend'] == self::BACKEND_REDSHIFT) {
            $this->markTestSkipped('Bug on Redshift backend');
        }
        $importFile = __DIR__ . '/../../_data/languages.csv';
        $tableId = $this->_client->createTableAsync(
            $this->getTestBucketId(),
            'languages',
            new CsvFile($importFile),
            [
                'primaryKey' => 'id,name',
            ]
        );

        $detail = $this->_client->getTable($tableId);

        $this->assertEquals(['id', 'name'], $detail['primaryKey']);

        try {
            $this->_client->deleteTableColumn($tableId, 'name');
            $this->fail('Column should not be deleted.');
        } catch (ClientException $e) {
            $this->assertEquals('storage.tables.cannotDeletePrimaryKeyColumn', $e->getStringCode());
        }
    }

    public function testPrimaryKeyAddRequiredParam(): void
    {
        $importFile = __DIR__ . '/../../_data/users.csv';
        $tableId = $this->_client->createTableAsync(
            $this->getTestBucketId(),
            'users',
            new CsvFile($importFile),
            []
        );

        $tableDetail = $this->_client->getTable($tableId);
        $this->assertArrayHasKey('primaryKey', $tableDetail);
        $this->assertEmpty($tableDetail['primaryKey']);

        try {
            $this->_client->createTablePrimaryKey($tableId, []);
            $this->fail('primary key should not be created');
        } catch (ClientException $e) {
            $this->assertEquals('storage.validation.primaryKey', $e->getStringCode());
        }
    }

    /**
     * Tests: https://github.com/keboola/connection/issues/246
     */
    public function testPrimaryKeyAddWithSameColumnsInDifferentBuckets(): void
    {
        $importFile = __DIR__ . '/../../_data/users.csv';

        $table1Id = $this->_client->createTableAsync(
            $this->getTestBucketId(),
            'users',
            new CsvFile($importFile)
        );

        $this->_client->addTableColumn($table1Id, 'new-column');

        $table2Id = $this->_client->createTableAsync(
            $this->getTestBucketId(self::STAGE_OUT),
            'users',
            new CsvFile($importFile)
        );

        $this->_client->createTablePrimaryKey($table2Id, ['id']);

        $table = $this->_client->getTable($table2Id);

        $this->assertEquals(['id'], $table['primaryKey']);
    }

    public function testPrimaryKeyAddWithDuplicty(): void
    {
        $this->skipTestForBackend([self::BACKEND_BIGQUERY,], 'BQ doesnt check duplicity in table');

        $primaryKeyColumns = ['id'];
        $importFile = __DIR__ . '/../../_data/users.csv';

        $tableId = $this->_client->createTableAsync(
            $this->getTestBucketId(),
            'users',
            new CsvFile($importFile),
            []
        );

        $this->_client->writeTableAsync(
            $tableId,
            new CsvFile($importFile),
            [
                'incremental' => true,
            ]
        );

        try {
            $this->_client->createTablePrimaryKey($tableId, $primaryKeyColumns);
        } catch (ClientException $e) {
            $this->assertEquals('storage.tables.primaryKeyDuplicateValues', $e->getStringCode());
        }

        // composite primary key
        $primaryKeyColumns = ['Id', 'Name'];
        $importFile = __DIR__ . '/../../_data/languages-more-columns.csv';

        $tableId = $this->_client->createTableAsync(
            $this->getTestBucketId(),
            'languages',
            new CsvFile($importFile),
            []
        );

        $this->_client->writeTableAsync(
            $tableId,
            new CsvFile($importFile),
            [
                'incremental' => true,
            ]
        );

        try {
            $this->_client->createTablePrimaryKey($tableId, $primaryKeyColumns);
            $this->fail('create should not be allowed');
        } catch (ClientException $e) {
            $this->assertEquals('storage.tables.primaryKeyDuplicateValues', $e->getStringCode());
        }
    }

    public function testPrimaryKeyDelete(): void
    {
        $importFile = __DIR__ . '/../../_data/users.csv';

        $tableId = $this->_client->createTableAsync(
            $this->getTestBucketId(),
            'users',
            new CsvFile($importFile),
            [
                'primaryKey' => 'id',
            ]
        );

        $aliasTableId = $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_OUT), $tableId);

        $tables = [
            $this->_client->getTable($tableId),
            $this->_client->getTable($aliasTableId),
        ];

        foreach ($tables as $tableDetail) {
            $this->assertArrayHasKey('primaryKey', $tableDetail);
            $this->assertEquals(['id'], $tableDetail['primaryKey']);
        }

        $this->_client->removeTablePrimaryKey($tableId);

        $tables = [
            $this->_client->getTable($tableId),
            $this->_client->getTable($aliasTableId),
        ];

        foreach ($tables as $tableDetail) {
            $this->assertArrayHasKey('primaryKey', $tableDetail);
            $this->assertEmpty($tableDetail['primaryKey']);
        }

        // composite primary key
        $importFile = __DIR__ . '/../../_data/languages-more-columns.csv';

        $tableId = $this->_client->createTableAsync(
            $this->getTestBucketId(),
            'languages',
            new CsvFile($importFile),
            [
                'primaryKey' => 'Id,Name',
            ]
        );

        $tableDetail = $this->_client->getTable($tableId);

        $this->assertArrayHasKey('primaryKey', $tableDetail);
        $this->assertEquals(['Id', 'Name'], $tableDetail['primaryKey']);

        $this->_client->removeTablePrimaryKey($tableId);

        $tableDetail = $this->_client->getTable($tableId);

        $this->assertArrayHasKey('primaryKey', $tableDetail);
        $this->assertEmpty($tableDetail['primaryKey']);

        // delete primary key from table with filtered alias
        $importFile = __DIR__ . '/../../_data/languages.more-columns.csv';

        $tableId = $this->_client->createTableAsync(
            $this->getTestBucketId(),
            'languages-more-columns',
            new CsvFile($importFile),
            [
                'primaryKey' => 'id',
            ]
        );

        $aliasTableId = $this->_client->createAliasTable(
            $this->getTestBucketId(self::STAGE_OUT),
            $tableId,
            null,
            [
                'aliasFilter' => [
                    'column' => 'id',
                    'values' => ['1'],
                ],
            ]
        );

        $tables = [
            $this->_client->getTable($tableId),
            $this->_client->getTable($aliasTableId),
        ];

        foreach ($tables as $tableDetail) {
            $this->assertArrayHasKey('primaryKey', $tableDetail);
            $this->assertEquals(['id'], $tableDetail['primaryKey']);
        }

        $indexRemoved = true;
        try {
            $this->_client->removeTablePrimaryKey($tableId);
        } catch (ClientException $e) {
            if ($e->getStringCode() == 'storage.tables.cannotRemoveReferencedColumnFromPrimaryKey') {
                $indexRemoved = false;
            } else {
                throw $e;
            }
        }

        // delete primary key from alias
        $this->assertFalse($indexRemoved);

        $tableDetail = $this->_client->getTable($tableId);

        $this->assertArrayHasKey('primaryKey', $tableDetail);
        $this->assertEquals(['id'], $tableDetail['primaryKey']);

        $indexRemoved = true;
        try {
            $this->_client->removeTablePrimaryKey($aliasTableId);
        } catch (ClientException $e) {
            if ($e->getStringCode() == 'storage.tables.aliasImportNotAllowed') {
                $indexRemoved = false;
            } else {
                throw $e;
            }
        }

        $this->assertFalse($indexRemoved);

        $tableDetail = $this->_client->getTable($aliasTableId);

        $this->assertArrayHasKey('primaryKey', $tableDetail);
        $this->assertEquals(['id'], $tableDetail['primaryKey']);
    }

    public function testEmptyPrimaryKeyDelete(): void
    {
        $tableId = $this->_client->createTableAsync(
            $this->getTestBucketId(),
            'users',
            new CsvFile(__DIR__ . '/../../_data/users.csv')
        );

        $this->_client->removeTablePrimaryKey($tableId);
        $table = $this->_client->getTable($tableId);
        $this->assertEmpty($table['primaryKey']);
    }

    public function testAddInvalidPrimaryKey(): void
    {
        $tableId = $this->_client->createTableAsync(
            $this->getTestBucketId(),
            'users',
            new CsvFile(__DIR__ . '/../../_data/users.csv')
        );
        try {
            $this->_client->createTablePrimaryKey($tableId, ['fakeColumn']);
            $this->fail('Adding invalid primary key should result in an error');
        } catch (ClientException $e) {
            $this->assertEquals('storage.validation.primaryKey', $e->getStringCode());
        }

        try {
            $this->_client->createTablePrimaryKey($tableId, ['id', 'fakeColumn']);
            $this->fail('Adding invalid primary key should result in an error');
        } catch (ClientException $e) {
            $this->assertEquals('storage.validation.primaryKey', $e->getStringCode());
        }
    }
}
