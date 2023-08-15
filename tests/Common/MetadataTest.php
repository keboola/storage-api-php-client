<?php

namespace Keboola\Test\Common;

use Generator;
use Keboola\Csv\CsvFile;
use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Metadata;
use Keboola\StorageApi\Options\Metadata\TableMetadataUpdateOptions;
use Keboola\StorageApi\Options\TokenAbstractOptions;
use Keboola\StorageApi\Options\TokenCreateOptions;
use Keboola\StorageApi\Tokens;
use Keboola\Test\ClientProvider\ClientProvider;
use Keboola\Test\ClientProvider\TestSetupHelper;
use Keboola\Test\StorageApiTestCase;

class MetadataTest extends StorageApiTestCase
{
    const TEST_PROVIDER = 'test';

    const TEST_METADATA_KEY_1 = 'test_metadata_key1';
    const TEST_METADATA_KEY_2 = 'test_metadata_key2';

    const ISO8601_REGEXP = '/^([0-9]{4})-(1[0-2]|0[1-9])-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})\+([0-9]{4})$/';
    // constants used for data providers in order to run it on all endpoints but also represents part of URL
    const ENDPOINT_TYPE_COLUMNS = 'columns';
    const ENDPOINT_TYPE_TABLES = 'tables';
    const ENDPOINT_TYPE_BUCKETS = 'buckets';

    private Client $_testClient;

    private ClientProvider $clientProvider;

    public function setUp(): void
    {
        parent::setUp();

        $this->clientProvider = new ClientProvider($this);
        [$devBranchType, $userRole] = $this->getProvidedData();
        [$this->_client, $this->_testClient] = (new TestSetupHelper())->setUpForProtectedDevBranch(
            $this->clientProvider,
            $devBranchType,
            $userRole,
        );

        if ($devBranchType === ClientProvider::DEV_BRANCH) {
            // buckets must be created in branch that the tests run in
            $this->initEmptyTestBucketsForParallelTests([self::STAGE_OUT, self::STAGE_IN], $this->_testClient);
        } elseif ($devBranchType === ClientProvider::DEFAULT_BRANCH) {
            $this->initEmptyTestBucketsForParallelTests();
        } else {
            throw new \Exception(sprintf('Unknown devBranchType "%s"', $devBranchType));
        }

        $metadataApi = new Metadata($this->_testClient);
        $metadatas = $metadataApi->listBucketMetadata($this->getTestBucketId());
        foreach ($metadatas as $md) {
            $metadataApi->deleteBucketMetadata($this->getTestBucketId(), $md['id']);
        }
        $this->_testClient->createTableAsync($this->getTestBucketId(), 'table', new CsvFile(__DIR__ . '/../_data/users.csv'));
    }

     /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     * @group SOX-66
     */
    public function testBucketMetadata(string $devBranchType, string $userRole): void
    {
        $bucketId = $this->getTestBucketId();
        $metadataApi = new Metadata($this->_testClient);

        $md = [
            'key' => self::TEST_METADATA_KEY_1,
            'value' => 'testval',
        ];
        $md2 = [
            'key' => self::TEST_METADATA_KEY_2,
            'value' => 'testval',
        ];
        $testMetadata = [$md, $md2];

        $provider = self::TEST_PROVIDER;
        $metadatas = $metadataApi->postBucketMetadata($bucketId, $provider, $testMetadata);

        $this->assertEquals(2, count($metadatas));
        $this->assertArrayHasKey('key', $metadatas[0]);
        $this->assertArrayHasKey('value', $metadatas[0]);
        $this->assertArrayHasKey('provider', $metadatas[0]);
        $this->assertArrayHasKey('timestamp', $metadatas[0]);
        $this->assertMatchesRegularExpression(self::ISO8601_REGEXP, $metadatas[0]['timestamp']);
        $this->assertEquals(self::TEST_PROVIDER, $metadatas[0]['provider']);

        $origValue = $metadatas[0]['value'];
        $mdCopy = $metadatas[0];
        $mdCopy['value'] = 'newValue';

        $newMetadata = $metadataApi->postBucketMetadata($bucketId, $provider, [$mdCopy]);

        foreach ($newMetadata as $metadata) {
            if ($metadata['id'] == $metadatas[0]['id']) {
                $this->assertEquals('newValue', $metadata['value']);
            } else {
                $this->assertEquals('testval', $metadata['value']);
            }
        }

        $metadataApi->deleteBucketMetadata($bucketId, $mdCopy['id']);

        $mdList = $metadataApi->listBucketMetadata($bucketId);

        $this->assertEquals(1, count($mdList));

        $this->assertEquals($metadatas[1]['key'], $mdList[0]['key']);
        $this->assertEquals($metadatas[1]['value'], $mdList[0]['value']);
        $this->assertEquals($metadatas[1]['provider'], $mdList[0]['provider']);
        $this->assertEquals($metadatas[1]['timestamp'], $mdList[0]['timestamp']);
    }

    /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     * @group SOX-66
     */
    public function testColumnMetadataOverwrite(string $devBranchType, string $userRole): void
    {
        $outTestBucketId = $this->getTestBucketId(self::STAGE_OUT);
        $outBucketTableId = $this->_testClient->createTableAsync(
            $outTestBucketId,
            'table',
            new CsvFile(__DIR__ . '/../_data/users.csv')
        );

        $outBucketColumnId = $outBucketTableId . '.id';
        $metadataApi = new Metadata($this->_testClient);

        $testMetadata = [
            [
                'key' => 'KBC.datatype.nullable',
                'value' => 'testValue',
            ],
        ];
        $metadata = $metadataApi->postBucketMetadata(
            $outTestBucketId,
            'user',
            $testMetadata
        );
        $this->assertSame($metadata[0]['value'], 'testValue');

        $metadata = $metadataApi->postTableMetadata(
            $outBucketTableId,
            'user',
            $testMetadata
        );
        $this->assertSame($metadata[0]['value'], 'testValue');

        $metadata = $metadataApi->postColumnMetadata(
            $outBucketColumnId,
            'user',
            $testMetadata
        );
        $this->assertSame($metadata[0]['value'], 'testValue');

        $columnId = $this->getMetadataTestColumnId('table', 'id');
        $metadataApi = new Metadata($this->_testClient);

        $testMetadata = [
            [
                'key' => 'KBC.datatype.nullable',
                'value' => 'true',
            ],
        ];
        $metadata = $metadataApi->postColumnMetadata(
            $columnId,
            'user',
            $testMetadata
        );

        $this->assertSame($metadata[0]['value'], 'true');
        // save same metadata with different value with different provider
        $testMetadata = [
            [
                'key' => 'KBC.datatype.nullable',
                'value' => 1,
            ],
        ];
        $metadata = $metadataApi->postColumnMetadata(
            $columnId,
            'transformation',
            $testMetadata
        );
        $this->assertSame($metadata[0]['value'], 'true');
        // repeat previous request
        $metadata = $metadataApi->postColumnMetadata(
            $columnId,
            'transformation',
            $testMetadata
        );
        // metadata from first request should not change
        $this->assertSame($metadata[0]['value'], 'true');

        $metadata = $metadataApi->listBucketMetadata($outTestBucketId);
        $this->assertSame($metadata[0]['value'], 'testValue');
        $metadata = $metadataApi->listTableMetadata($outBucketTableId);
        $this->assertSame($metadata[0]['value'], 'testValue');
        $metadata = $metadataApi->listColumnMetadata($outBucketColumnId);
        $this->assertSame($metadata[0]['value'], 'testValue');
    }

     /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     * @group SOX-66
     */
    public function testTableMetadata(string $devBranchType, string $userRole): void
    {
        $tableId = $this->getMetadataTestTableId('table');
        $metadataApi = new Metadata($this->_testClient);

        $md = [
            'key' => self::TEST_METADATA_KEY_1,
            'value' => 'testval',
        ];
        $md2 = [
            'key' => self::TEST_METADATA_KEY_2,
            'value' => 'testval',
        ];
        $testMetadata = [$md, $md2];

        $provider = self::TEST_PROVIDER;

        $metadatas = $metadataApi->postTableMetadata($tableId, $provider, $testMetadata);

        $this->assertEquals(2, count($metadatas));
        $this->assertArrayHasKey('key', $metadatas[0]);
        $this->assertArrayHasKey('value', $metadatas[0]);
        $this->assertArrayHasKey('provider', $metadatas[0]);
        $this->assertArrayHasKey('timestamp', $metadatas[0]);
        $this->assertMatchesRegularExpression(self::ISO8601_REGEXP, $metadatas[0]['timestamp']);
        $this->assertEquals(self::TEST_PROVIDER, $metadatas[0]['provider']);

        $mdCopy = $metadatas[0];
        $mdCopy['value'] = 'newValue';

        $newMetadata = $metadataApi->postTableMetadata($tableId, $provider, [$mdCopy]);

        foreach ($newMetadata as $metadata) {
            if ($metadata['id'] == $metadatas[0]['id']) {
                $this->assertEquals('newValue', $metadata['value']);
                $this->assertGreaterThanOrEqual(
                    strtotime($metadatas[0]['timestamp']),
                    strtotime($metadata['timestamp'])
                );
            } else {
                $this->assertEquals('testval', $metadata['value']);
            }
        }

        $metadataApi->deleteTableMetadata($tableId, $mdCopy['id']);

        $mdList = $metadataApi->listTableMetadata($tableId);

        $this->assertEquals(1, count($mdList));

        $this->assertEquals($metadatas[1]['key'], $mdList[0]['key']);
        $this->assertEquals($metadatas[1]['value'], $mdList[0]['value']);
        $this->assertEquals($metadatas[1]['provider'], $mdList[0]['provider']);
        $this->assertEquals($metadatas[1]['timestamp'], $mdList[0]['timestamp']);

        // test that bucket metadata is included in the get table api response
        $bucketMetadata = [$md];
        $metadataApi->postBucketMetadata($this->getTestBucketId(), $provider, $bucketMetadata);

        $table = $this->_testClient->getTable($tableId);
        $this->assertArrayHasKey('metadata', $table['bucket']);
        $this->assertCount(1, $table['bucket']['metadata']);
        $this->assertEquals($table['bucket']['metadata'][0]['key'], $md['key']);
        $this->assertEquals($table['bucket']['metadata'][0]['value'], $md['value']);
    }

    /**
     * @return void
     */
     /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     * @group SOX-66
     */
    public function testTableMetadataWithColumns(string $devBranchType, string $userRole): void
    {
        $tableId = $this->getMetadataTestTableId('table');
        $column1 = 'id';
        $column2 = 'name';
        $metadataApi = new Metadata($this->_testClient);

        $md = [
            'key' => self::TEST_METADATA_KEY_1,
            'value' => 'testval',
        ];
        $md2 = [
            'key' => self::TEST_METADATA_KEY_2,
            'value' => 'testval',
        ];
        $testMetadata = [
            $md,
            $md2,
        ];
        $testColumnsMetadata = [
            $column1 => [
                $md,
                $md2,
            ],
            $column2 => [
                $md,
            ],
        ];

        $provider = self::TEST_PROVIDER;

        // post metadata
        $options = new TableMetadataUpdateOptions($tableId, $provider, $testMetadata, $testColumnsMetadata);
        /** @var array $metadatas */
        $metadatas = $metadataApi->postTableMetadataWithColumns($options);

        $this->assertCount(2, $metadatas);
        $this->assertArrayHasKey('metadata', $metadatas);
        $this->assertArrayHasKey('columnsMetadata', $metadatas);
        // check table metadata
        $metadata = $metadatas['metadata'];
        $this->assertCount(2, $metadata);
        $this->assertArrayHasKey('key', $metadata[0]);
        $this->assertArrayHasKey('value', $metadata[0]);
        $this->assertArrayHasKey('provider', $metadata[0]);
        $this->assertArrayHasKey('timestamp', $metadata[0]);
        $this->assertMatchesRegularExpression(self::ISO8601_REGEXP, $metadata[0]['timestamp']);
        $this->assertEquals(self::TEST_PROVIDER, $metadata[0]['provider']);
        // check columns metadata
        $columns = $metadatas['columnsMetadata'];
        $this->assertCount(2, $columns);
        $this->assertArrayHasKey($column1, $columns);
        $this->assertArrayHasKey($column2, $columns);
        // check column 1
        $metadata = $metadatas['columnsMetadata'][$column1];
        $this->assertCount(2, $metadata);
        $this->assertArrayHasKey('key', $metadata[0]);
        $this->assertArrayHasKey('value', $metadata[0]);
        $this->assertArrayHasKey('provider', $metadata[0]);
        $this->assertArrayHasKey('timestamp', $metadata[0]);
        $this->assertMatchesRegularExpression(self::ISO8601_REGEXP, $metadata[0]['timestamp']);
        $this->assertEquals(self::TEST_PROVIDER, $metadata[0]['provider']);
        $this->assertArrayHasKey('key', $metadata[1]);
        $this->assertArrayHasKey('value', $metadata[1]);
        $this->assertArrayHasKey('provider', $metadata[1]);
        $this->assertArrayHasKey('timestamp', $metadata[1]);
        $this->assertMatchesRegularExpression(self::ISO8601_REGEXP, $metadata[1]['timestamp']);
        $this->assertEquals(self::TEST_PROVIDER, $metadata[1]['provider']);
        // check column 2
        $metadata = $metadatas['columnsMetadata'][$column2];
        $this->assertCount(1, $metadata);
        $this->assertArrayHasKey('key', $metadata[0]);
        $this->assertArrayHasKey('value', $metadata[0]);
        $this->assertArrayHasKey('provider', $metadata[0]);
        $this->assertArrayHasKey('timestamp', $metadata[0]);
        $this->assertMatchesRegularExpression(self::ISO8601_REGEXP, $metadata[0]['timestamp']);
        $this->assertEquals(self::TEST_PROVIDER, $metadata[0]['provider']);

        // copy metadata
        $mdCopy = [];
        $mdCopy['key'] = $metadatas['metadata'][0]['key'];
        $mdCopy['value'] = 'newValue';

        // copy column metadata
        $mdColumnCopy = [];
        $mdColumnCopy['key'] = $metadatas['columnsMetadata'][$column1][0]['key'];
        $mdColumnCopy['value'] = 'newValue';

        // post copied metadata
        $options = new TableMetadataUpdateOptions($tableId, $provider, [$mdCopy], [$column1 => [$mdColumnCopy]]);
        /** @var array $newMetadatas */
        $newMetadatas = $metadataApi->postTableMetadataWithColumns($options);

        // check table metadata
        foreach ($newMetadatas['metadata'] as $metadata) {
            if ($metadata['id'] == $metadatas['metadata'][0]['id']) {
                $this->assertEquals('newValue', $metadata['value']);
                $this->assertGreaterThanOrEqual(strtotime($metadatas['metadata'][0]['timestamp']), strtotime($metadata['timestamp']));
            } else {
                $this->assertEquals('testval', $metadata['value']);
            }
        }

        $columns = $metadatas['columnsMetadata'];
        // we did send only one column metadata, but there are still two columns in metadata
        $this->assertCount(2, $columns);

        // check columns metadata
        foreach ($newMetadatas['columnsMetadata'] as $columnName => $columnMetadatas) {
            foreach ($columnMetadatas as $metadata) {
                if ($metadata['id'] == $metadatas['columnsMetadata'][$column1][0]['id']) {
                    $this->assertEquals('newValue', $metadata['value']);
                    $this->assertGreaterThanOrEqual(strtotime($metadatas['columnsMetadata'][$column1][0]['timestamp']), strtotime($metadata['timestamp']));
                } else {
                    $this->assertEquals('testval', $metadata['value']);
                }
            }
        }
    }

     /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     * @group SOX-66
     */
    public function testColumnMetadataWithColumnsWithIntegers(string $devBranchType, string $userRole): void
    {
        $this->_testClient->createTableAsync($this->getTestBucketId(), 'tableWithIntColumns', new CsvFile(__DIR__ . '/../_data/numbers.two-cols.csv'));

        $tableId = $this->getMetadataTestTableId('tableWithIntColumns');
        $column1 = '0';
        $columnId = $this->getMetadataTestColumnId('tableWithIntColumns', $column1);

        $metadataApi = new Metadata($this->_testClient);

        $mdForTable = [
            'key' => self::TEST_METADATA_KEY_1,
            'value' => 'tableValue',
        ];

        $mdForColumn = [
            'key' => self::TEST_METADATA_KEY_2,
            'value' => 'columnValue',
        ];

        $provider = self::TEST_PROVIDER;

        $metadataApi->postTableMetadata($tableId, $provider, [$mdForTable]);
        $tableDetail = $this->_testClient->getTable($tableId);

        $this->assertNotEmpty($tableDetail['metadata']);
        $this->assertCount(1, $tableDetail['metadata']);
        $this->assertCount(0, $tableDetail['columnMetadata']);

        $metadataApi->postColumnMetadata($columnId, $provider, [$mdForColumn]);
        $tableDetail = $this->_testClient->getTable($tableId);

        $this->assertNotEmpty($tableDetail['metadata']);
        $this->assertCount(1, $tableDetail['metadata']);

        $this->assertArrayEqualsExceptKeys($mdForTable, $tableDetail['metadata'][0], ['id', 'provider', 'timestamp']);

        $this->assertCount(1, $tableDetail['columnMetadata']);
        $this->assertArrayEqualsExceptKeys($mdForColumn, $tableDetail['columnMetadata'][0][0], ['id', 'provider', 'timestamp']);
    }

     /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     * @group SOX-66
     */
    public function testTableMetadataWithColumnsWithIntegers(string $devBranchType, string $userRole): void
    {
        $this->_testClient->createTableAsync($this->getTestBucketId(), 'tableWithIntColumns', new CsvFile(__DIR__ . '/../_data/numbers.two-cols.csv'));

        $tableId = $this->getMetadataTestTableId('tableWithIntColumns');
        $column1 = '0';
        $column2 = '45';
        $metadataApi = new Metadata($this->_testClient);

        $md = [
            'key' => self::TEST_METADATA_KEY_1,
            'value' => 'testvalA',
        ];

        $md2 = [
            'key' => self::TEST_METADATA_KEY_2,
            'value' => 'testvalB',
        ];

        $testMetadata = [
            $md,
            $md2,
        ];
        $testColumnsMetadata = [
            $column1 => [
                $md,
                $md2,
            ],
            $column2 => [
                [
                    'key' => self::TEST_METADATA_KEY_1,
                    'value' => 'testvalA',
                    'columnName' => '45',
                ],
            ],
        ];

        $provider = self::TEST_PROVIDER;

        // post metadata
        $options = new TableMetadataUpdateOptions($tableId, $provider, $testMetadata, $testColumnsMetadata);
        /** @var array $metadatas */
        $metadatas = $metadataApi->postTableMetadataWithColumns($options);

        $this->assertEquals(2, count($metadatas));
        $this->assertArrayHasKey('metadata', $metadatas);
        $this->assertArrayHasKey('columnsMetadata', $metadatas);
        // check table metadata
        $metadata = $metadatas['metadata'];
        $this->assertEquals(2, count($metadata));
        $this->assertArrayHasKey('key', $metadata[0]);
        $this->assertArrayHasKey('value', $metadata[0]);
        $this->assertArrayHasKey('provider', $metadata[0]);
        $this->assertArrayHasKey('timestamp', $metadata[0]);
        $this->assertMatchesRegularExpression(self::ISO8601_REGEXP, $metadata[0]['timestamp']);
        $this->assertEquals(self::TEST_PROVIDER, $metadata[0]['provider']);
        // check columns metadata
        $columns = $metadatas['columnsMetadata'];
        $this->assertEquals(2, count($columns));
        $this->assertArrayHasKey($column1, $columns);
        $this->assertArrayHasKey($column2, $columns);
        // check column 1
        $metadata = $metadatas['columnsMetadata'][$column1];
        $this->assertEquals(2, count($metadata));
        $this->assertArrayHasKey('key', $metadata[0]);
        $this->assertArrayHasKey('value', $metadata[0]);
        $this->assertArrayHasKey('provider', $metadata[0]);
        $this->assertArrayHasKey('timestamp', $metadata[0]);
        $this->assertMatchesRegularExpression(self::ISO8601_REGEXP, $metadata[0]['timestamp']);
        $this->assertEquals(self::TEST_PROVIDER, $metadata[0]['provider']);
        $this->assertArrayHasKey('key', $metadata[1]);
        $this->assertArrayHasKey('value', $metadata[1]);
        $this->assertArrayHasKey('provider', $metadata[1]);
        $this->assertArrayHasKey('timestamp', $metadata[1]);
        $this->assertMatchesRegularExpression(self::ISO8601_REGEXP, $metadata[1]['timestamp']);
        $this->assertEquals(self::TEST_PROVIDER, $metadata[1]['provider']);
        // check column 2
        $metadata = $metadatas['columnsMetadata'][$column2];
        $this->assertEquals(1, count($metadata));
        $this->assertArrayHasKey('key', $metadata[0]);
        $this->assertArrayHasKey('value', $metadata[0]);
        $this->assertArrayHasKey('provider', $metadata[0]);
        $this->assertArrayHasKey('timestamp', $metadata[0]);
        $this->assertMatchesRegularExpression(self::ISO8601_REGEXP, $metadata[0]['timestamp']);
        $this->assertEquals(self::TEST_PROVIDER, $metadata[0]['provider']);
    }

     /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     * @group SOX-66
     */
    public function testTableMetadataForTokenWithReadPrivilege(string $devBranchType, string $userRole): void
    {
        $testMetadataValue = 'testval';

        $bucketId = $this->getTestBucketId();
        $tableId = $this->getMetadataTestTableId('table');
        $metadataApi = new Metadata($this->_testClient);

        $provider = self::TEST_PROVIDER;
        $metadataApi->postTableMetadata(
            $tableId,
            $provider,
            [
                [
                    'key' => self::TEST_METADATA_KEY_1,
                    'value' => $testMetadataValue,
                ],
            ]
        );

        $readClient = $this->getReadClient($devBranchType, $bucketId, $userRole);

        $readMetadataApi = new Metadata($readClient);

        $metadataArray = $readMetadataApi->listTableMetadata($tableId);
        $this->assertCount(1, $metadataArray);

        $metadata = $metadataArray[0];
        $this->assertEquals($testMetadataValue, $metadata['value']);

        // delete
        try {
            $readMetadataApi->deleteTableMetadata($tableId, $metadata['id']);
            $this->fail('Token with read permissions should not delete metadata');
        } catch (ClientException $e) {
            $this->assertEquals(403, $e->getCode());
        }

        $metadataArray = $readMetadataApi->listTableMetadata($tableId);
        $this->assertCount(1, $metadataArray);

        // update
        try {
            $readMetadataApi->postTableMetadata(
                $tableId,
                $provider,
                [
                    [
                        'key' => self::TEST_METADATA_KEY_1,
                        'value' => 'changed',
                    ],
                ]
            );

            $this->fail('Token with read permissions should not update metadata');
        } catch (ClientException $e) {
            $this->assertEquals(403, $e->getCode());
        }

        $metadataArray = $readMetadataApi->listTableMetadata($tableId);
        $this->assertCount(1, $metadataArray);

        $metadata = $metadataArray[0];
        $this->assertEquals($testMetadataValue, $metadata['value']);

        // new metadata
        try {
            $readMetadataApi->postTableMetadata(
                $tableId,
                $provider,
                [
                    [
                        'key' => self::TEST_METADATA_KEY_2,
                        'value' => $testMetadataValue,
                    ],
                ]
            );

            $this->fail('Token with read permissions should not create metadata');
        } catch (ClientException $e) {
            $this->assertEquals(403, $e->getCode());
        }

        $metadataArray = $readMetadataApi->listTableMetadata($tableId);
        $this->assertCount(1, $metadataArray);
    }

     /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     * @group SOX-66
     */
    public function testTableDeleteWithMetadata(string $devBranchType, string $userRole): void
    {
        $tableId = $this->getMetadataTestTableId('table');
        $columnId = $this->getMetadataTestColumnId('table', 'sex');
        $metadataApi = new Metadata($this->_testClient);

        $md = [
            'key' => self::TEST_METADATA_KEY_1,
            'value' => 'testval',
        ];
        $md2 = [
            'key' => self::TEST_METADATA_KEY_2,
            'value' => 'testval',
        ];
        $testMetadata = [$md, $md2];

        $provider = self::TEST_PROVIDER;

        $metadataApi->postTableMetadata($tableId, $provider, $testMetadata);
        $tableDetail = $this->_testClient->getTable($tableId);

        $this->assertNotEmpty($tableDetail['metadata']);
        $this->assertCount(2, $tableDetail['metadata']);

        $metadataApi->postColumnMetadata($columnId, $provider, $testMetadata);
        $tableDetail = $this->_testClient->getTable($tableId);

        $this->assertNotEmpty($tableDetail['columnMetadata']);
        $this->assertCount(2, $tableDetail['columnMetadata']['sex']);

        $this->_testClient->dropTable($tableId);

        $this->expectException(ClientException::class);
        $this->expectExceptionCode(404);
        $metadataApi->listTableMetadata($tableId);

        $this->expectException(ClientException::class);
        $this->expectExceptionCode(404);
        $metadataApi->listTableMetadata($columnId);
    }

     /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     * @group SOX-66
     */
    public function testColumnMetadata(string $devBranchType, string $userRole): void
    {
        $columnId = $this->getMetadataTestColumnId('table', 'id');
        $metadataApi = new Metadata($this->_testClient);

        $md = [
            'key' => self::TEST_METADATA_KEY_1,
            'value' => 'testval',
        ];
        $md2 = [
            'key' => self::TEST_METADATA_KEY_2,
            'value' => 'testval',
        ];
        $testMetadata = [$md, $md2];

        $provider = self::TEST_PROVIDER;

        $metadatas = $metadataApi->postColumnMetadata($columnId, $provider, $testMetadata);

        $this->assertCount(2, $metadatas);
        $this->assertArrayHasKey('key', $metadatas[0]);
        $this->assertArrayHasKey('value', $metadatas[0]);
        $this->assertArrayHasKey('provider', $metadatas[0]);
        $this->assertArrayHasKey('timestamp', $metadatas[0]);
        $this->assertMatchesRegularExpression(self::ISO8601_REGEXP, $metadatas[0]['timestamp']);
        $this->assertEquals(self::TEST_PROVIDER, $metadatas[0]['provider']);

        $mdCopy = $metadatas[0];
        $mdCopy['value'] = 'newValue';

        $newMetadata = $metadataApi->postColumnMetadata($columnId, $provider, [$mdCopy]);
        // we send only one record which will be updated, but second record is still same
        $this->assertCount(2, $newMetadata);
        foreach ($newMetadata as $metadata) {
            if ($metadata['id'] == $metadatas[0]['id']) {
                $this->assertEquals('newValue', $metadata['value']);
                $this->assertGreaterThanOrEqual(
                    strtotime($metadatas[0]['timestamp']),
                    strtotime($metadata['timestamp'])
                );
            } else {
                $this->assertEquals('testval', $metadata['value']);
            }
        }

        $metadataApi->deleteColumnMetadata($columnId, $mdCopy['id']);

        $mdList = $metadataApi->listColumnMetadata($columnId);

        $this->assertCount(1, $mdList);

        $this->assertEquals($metadatas[1]['key'], $mdList[0]['key']);
        $this->assertEquals($metadatas[1]['value'], $mdList[0]['value']);
        $this->assertEquals($metadatas[1]['provider'], $mdList[0]['provider']);
        $this->assertEquals($metadatas[1]['timestamp'], $mdList[0]['timestamp']);

        if ($devBranchType === ClientProvider::DEV_BRANCH) {
            $this->markTestIncomplete('Table alias not implemented in dev branch.');
        }

        // create alias of alias
        $this->_testClient->createAliasTable(
            $this->getTestBucketId(),
            $this->getMetadataTestTableId('table'),
            'tableAlias'
        );
        $this->_testClient->createAliasTable(
            $this->getTestBucketId(),
            $this->getMetadataTestTableId('tableAlias'),
            'tableAliasAlias'
        );

        // test list tables call
        $tables = $this->_testClient->listTables(null, ['include' => 'columnMetadata']);
        // call return all tables, filter the alias of alias one

        $aliasAliasTableId = $this->getMetadataTestTableId('tableAliasAlias');
        $tables = array_values(array_filter($tables, function ($table) use ($aliasAliasTableId) {
            return $table['id'] === $aliasAliasTableId;
        }));

        // the metadata should be propagated from the source table
        $this->assertNotEmpty($tables[0]['sourceTable']['columnMetadata']['id']);
        $this->assertEquals(
            $mdList,
            $tables[0]['sourceTable']['columnMetadata']['id']
        );

        $alias = $this->_testClient->getTable($aliasAliasTableId);
        $this->assertNotEmpty($alias['sourceTable']['columnMetadata']);
        $this->assertEquals(
            $mdList,
            $alias['sourceTable']['columnMetadata']['id']
        );

        $tables = $this->_testClient->listTables($this->getTestBucketId(), ['include' => 'columnMetadata']);
        $tables = array_values(array_filter($tables, function ($table) use ($aliasAliasTableId) {
            return $table['id'] === $aliasAliasTableId;
        }));
        $this->assertEquals(
            $mdList,
            $tables[0]['sourceTable']['columnMetadata']['id']
        );
    }

     /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     * @group SOX-66
     */
    public function testColumnMetadataForTokenWithReadPrivilege(string $devBranchType, string $userRole): void
    {
        $testMetadataValue = 'testval';

        $bucketId = $this->getTestBucketId();
        $columnId = $this->getMetadataTestColumnId('table', 'id');
        $metadataApi = new Metadata($this->_testClient);

        $provider = self::TEST_PROVIDER;
        $metadataApi->postColumnMetadata(
            $columnId,
            $provider,
            [
                [
                    'key' => self::TEST_METADATA_KEY_1,
                    'value' => $testMetadataValue,
                ],
            ]
        );

        $readClient = $this->getReadClient($devBranchType, $bucketId, $userRole);

        $readMetadataApi = new Metadata($readClient);

        $metadataArray = $readMetadataApi->listColumnMetadata($columnId);
        $this->assertCount(1, $metadataArray);

        $metadata = $metadataArray[0];
        $this->assertEquals($testMetadataValue, $metadata['value']);

        // delete
        try {
            $readMetadataApi->deleteColumnMetadata($columnId, $metadata['id']);
            $this->fail('Token with read permissions should not delete metadata');
        } catch (ClientException $e) {
            $this->assertEquals(403, $e->getCode());
        }

        $metadataArray = $readMetadataApi->listColumnMetadata($columnId);
        $this->assertCount(1, $metadataArray);

        // update
        try {
            $readMetadataApi->postColumnMetadata(
                $columnId,
                $provider,
                [
                    [
                        'key' => self::TEST_METADATA_KEY_1,
                        'value' => 'changed',
                    ],
                ]
            );

            $this->fail('Token with read permissions should not update metadata');
        } catch (ClientException $e) {
            $this->assertEquals(403, $e->getCode());
        }

        $metadataArray = $readMetadataApi->listColumnMetadata($columnId);
        $this->assertCount(1, $metadataArray);

        $metadata = $metadataArray[0];
        $this->assertEquals($testMetadataValue, $metadata['value']);

        // new metadata
        try {
            $readMetadataApi->postColumnMetadata(
                $columnId,
                $provider,
                [
                    [
                        'key' => self::TEST_METADATA_KEY_2,
                        'value' => $testMetadataValue,
                    ],
                ]
            );

            $this->fail('Token with read permissions should not create metadata');
        } catch (ClientException $e) {
            $this->assertEquals(403, $e->getCode());
        }

        $metadataArray = $readMetadataApi->listColumnMetadata($columnId);
        $this->assertCount(1, $metadataArray);
    }

     /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     * @group SOX-66
     */
    public function testTableColumnDeleteWithMetadata(string $devBranchType, string $userRole): void
    {
        $tableId = $this->getMetadataTestTableId('table');
        $columnId = $this->getMetadataTestColumnId('table', 'sex');
        $metadataApi = new Metadata($this->_testClient);

        $md = [
            'key' => self::TEST_METADATA_KEY_1,
            'value' => 'testval',
        ];
        $md2 = [
            'key' => self::TEST_METADATA_KEY_2,
            'value' => 'testval',
        ];
        $testMetadata = [$md, $md2];

        $provider = self::TEST_PROVIDER;

        $metadataApi->postColumnMetadata($columnId, $provider, $testMetadata);

        $tableDetail = $this->_testClient->getTable($tableId);

        $this->assertNotEmpty($tableDetail['columnMetadata']);
        $this->assertEquals(2, count($tableDetail['columnMetadata']['sex']));

        $this->_testClient->deleteTableColumn($tableId, 'sex');

        $tableDetail = $this->_testClient->getTable($tableId);
        $this->assertEmpty($tableDetail['columnMetadata']);

        $this->assertEquals(['id','name','city'], $tableDetail['columns']);

        $this->expectException(ClientException::class);
        $this->expectExceptionCode(404);

        $metadataApi->listColumnMetadata($columnId);
    }

     /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     * @group SOX-66
     */
    public function testUpdateTimestamp(string $devBranchType, string $userRole): void
    {
        $bucketId = $this->getTestBucketId();
        $metadataApi = new Metadata($this->_testClient);

        $md = [
            'key' => self::TEST_METADATA_KEY_1,
            'value' => 'testval',
        ];
        $md2 = [
            'key' => self::TEST_METADATA_KEY_1,
            'value' => 'new testval',
        ];
        $testMetadata = [$md];

        $provider = self::TEST_PROVIDER;
        $metadatas = $metadataApi->postBucketMetadata($bucketId, $provider, $testMetadata);

        $this->assertCount(1, $metadatas);
        $this->assertArrayHasKey('timestamp', $metadatas[0]);
        $timestamp1 = $metadatas[0]['timestamp'];

        // just to ensure that the updated timestamp will be a few seconds greater
        sleep(5);

        $newMetadatas = $metadataApi->postBucketMetadata($bucketId, $provider, [$md2]);
        $this->assertCount(1, $newMetadatas);
        $this->assertArrayHasKey('timestamp', $newMetadatas[0]);
        $timestamp2 = $newMetadatas[0]['timestamp'];

        $this->assertGreaterThan(strtotime($timestamp1), strtotime($timestamp2));
    }

    /**
     * @dataProvider apiEndpoints
     * @group SOX-66
     */
    public function testInvalidMetadata(string $devBranchType, string $userRole, $apiEndpoint, $object): void
    {
        $bucketId = self::getTestBucketId();
        $object = ($apiEndpoint === self::ENDPOINT_TYPE_BUCKETS) ? $bucketId : $bucketId . $object;

        $md = [
            'key' => '%invalidKey', // invalid char %
            'value' => 'testval',
        ];

        try {
            // this should fail because metadata objects must be provided in an array
            $this->postMetadata($apiEndpoint, $object, $md);
            $this->fail('metadata must be an array of key-value objects.');
        } catch (ClientException $e) {
            $this->assertEquals('storage.metadata.invalidStructure', $e->getStringCode());
            $this->assertEquals(
                "Invalid structure. Metadata must be provided as an array of objects with 'key' and 'value' members",
                $e->getMessage()
            );
        }

        try {
            $this->postMetadata($apiEndpoint, $object, [$md]);
            $this->fail('Should throw invalid key exception');
        } catch (ClientException $e) {
            $this->assertEquals('storage.metadata.invalidKey', $e->getStringCode());
            $this->assertEquals('Invalid Metadata Key (metadata[0][key])', $e->getMessage());
        }

        // length > 255
        $invalidKey = str_pad('validKey', 260, '+');
        $md = [
            'key' => $invalidKey,
            'value' => 'testval',
        ];
        try {
            $this->postMetadata($apiEndpoint, $object, [$md]);
            $this->fail('Should throw invalid key exception');
        } catch (ClientException $e) {
            $this->assertEquals('storage.metadata.invalidKey', $e->getStringCode());
            $this->assertEquals('Invalid Metadata Key (metadata[0][key])', $e->getMessage());
        }

        $md = [
            'key' => '', // empty key
            'value' => 'testval',
        ];
        try {
            $this->postMetadata($apiEndpoint, $object, [$md]);
            $this->fail('Should throw invalid key exception');
        } catch (ClientException $e) {
            $this->assertEquals('storage.metadata.invalidKey', $e->getStringCode());
            $this->assertEquals('Invalid Metadata Key (metadata[0][key])', $e->getMessage());
        }
    }

    /**
     * $apiEndpoint represents part of URl because it has to call apiPost() directly without postMetadata() because
     * postMetadata() checks input in on client side, but this test should call it with wrong data in order to test it
     * in connection
     *
     * @dataProvider apiEndpoints
     * @group SOX-66
     */
    public function testInvalidMetadataWhenMetadataIsNotArray(string $devBranchType, string $userRole, $apiEndpoint, $object): void
    {
        $bucketId = self::getTestBucketId();
        $objectId = $bucketId . $object;

        try {
            // generating different string code to each run
            $this->_testClient->apiPost("{$apiEndpoint}/{$objectId}/metadata", [
                'provider' => 'valid',
                'metadata' => 'not an array',
            ]);
            $this->fail('Should throw invalid key exception');
        } catch (ClientException $e) {
            $this->assertEquals('storage.metadata.invalidParameter', $e->getStringCode());
            $this->assertEquals('The metadata parameter must be an array.', $e->getMessage());
        }
    }

    /**
     * @dataProvider apiEndpoints
     * @group SOX-66
     */
    public function testInvalidMetadataWhenMetadataIsNotPresent(string $devBranchType, string $userRole, $apiEndpoint, $object): void
    {
        $bucketId = self::getTestBucketId();
        $objectId = $bucketId . $object;

        try {
            $this->_testClient->apiPost("{$apiEndpoint}/{$objectId}/metadata", [
                'provider' => 'valid',
            ]);
            $this->fail('Should throw invalid key exception');
        } catch (ClientException $e) {
            $this->assertEquals('storage.metadata.invalidParameter', $e->getStringCode());
            $this->assertEquals('The metadata parameter must be an array.', $e->getMessage());
        }
    }

    /**
     * @dataProvider apiEndpoints
     * @group SOX-66
     */
    public function testMetadata40xs(string $devBranchType, string $userRole, $apiEndpoint, $object): void
    {
        $bucketId = self::getTestBucketId();
        $object = ($apiEndpoint === 'bucket') ? $bucketId : $bucketId . $object;

        // test invalid number
        try {
            $this->deleteMetadata($apiEndpoint, $object, 9999999);
            $this->fail('Invalid metadataId');
        } catch (ClientException $e) {
            $this->assertEquals(404, $e->getCode());
            $this->assertEquals('storage.metadata.notFound', $e->getStringCode());
            $this->assertEquals('The supplied metadata ID was not found', $e->getMessage());
        }

        // test null
        try {
            $this->deleteMetadata($apiEndpoint, $object, null);
            $this->fail('Invalid metadataId');
        } catch (ClientException $e) {
            $this->assertEquals(400, $e->getCode());
            $this->assertEquals('APPLICATION_ERROR', $e->getStringCode());
            $this->assertEquals('exceptions.storage.metadata.invalidDelete', $e->getMessage());
        }

        // not numeric value
        try {
            $this->deleteMetadata($apiEndpoint, $object, 'notNumber');
            $this->fail('Invalid metadataId');
        } catch (ClientException $e) {
            $this->assertEquals(400, $e->getCode());
            $this->assertEquals('APPLICATION_ERROR', $e->getStringCode());
            $this->assertEquals(
                'Argument "subResource" is expected to be type "int", value "notNumber" given.',
                $e->getMessage()
            );
        }
    }

     /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     * @group SOX-66
     */
    public function testInvalidProvider(string $devBranchType, string $userRole): void
    {
        $metadataApi = new Metadata($this->_testClient);
        $md = [
            'key' => 'validKey',
            'value' => 'testval',
        ];

        try {
            // provider null should be rejected
            /** @phpstan-ignore-next-line */
            $metadataApi->postBucketMetadata($this->getTestBucketId(), null, [$md]);
            $this->fail('provider is required');
        } catch (ClientException $e) {
            $this->assertEquals('storage.metadata.invalidProvider', $e->getStringCode());
            $this->assertEquals('Provider is required.', $e->getMessage());
        }

        // invalid characters in provider
        try {
            $metadataApi->postBucketMetadata($this->getTestBucketId(), '%invalidCharacter$', [$md]);
            $this->fail('Invalid metadata provider');
        } catch (ClientException $e) {
            $this->assertEquals('storage.metadata.invalidProvider', $e->getStringCode());
            $this->assertEquals('Invalid metadata provider.', $e->getMessage());
        }
    }

     /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     * @group SOX-66
     */
    public function testTryToRemoveForeignData(string $devBranchType, string $userRole): void
    {
        $medataApi = new Metadata($this->_testClient);
        $md = [
            'key' => 'magic-key',
            'value' => 'magic-frog',
        ];

        $createdMetadata = $medataApi->postBucketMetadata($this->getTestBucketId(), 'provider', [$md]);
        $anotherBucketId = $this->getTestBucketId(self::STAGE_OUT);

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('The supplied metadata ID was not found');
        $medataApi->deleteBucketMetadata($anotherBucketId, $createdMetadata[0]['id']);
    }

     /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     * @group SOX-66
     */
    public function testTryToRemoveForeignMetadataFromTable(string $devBranchType, string $userRole): void
    {
        $medataApi = new Metadata($this->_testClient);
        $md = [
            'key' => 'magic-key',
            'value' => 'magic-frog',
        ];

        $tableId = $this->getMetadataTestTableId('table');
        $createdMetadata = $medataApi->postTableMetadata($tableId, 'provider', [$md]);

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('The supplied metadata ID was not found');
        $medataApi->deleteBucketMetadata($this->getTestBucketId(), $createdMetadata[0]['id']);
    }

     /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     * @group SOX-66
     */
    public function testBucketMetadataForTokenWithReadPrivilege(string $devBranchType, string $userRole): void
    {
        $testMetadataValue = 'testval';

        $bucketId = $this->getTestBucketId();
        $metadataApi = new Metadata($this->_testClient);

        $provider = self::TEST_PROVIDER;
        $metadataApi->postBucketMetadata(
            $bucketId,
            $provider,
            [
                [
                    'key' => self::TEST_METADATA_KEY_1,
                    'value' => $testMetadataValue,
                ],
            ]
        );

        $readClient = $this->getReadClient($devBranchType, $bucketId);

        $readMetadataApi = new Metadata($readClient);

        $metadataArray = $readMetadataApi->listBucketMetadata($bucketId);
        $this->assertCount(1, $metadataArray);

        $metadata = $metadataArray[0];
        $this->assertEquals($testMetadataValue, $metadata['value']);

        // delete
        try {
            $readMetadataApi->deleteBucketMetadata($bucketId, $metadata['id']);
            $this->fail('Token with read permissions should not delete metadata');
        } catch (ClientException $e) {
            $this->assertEquals(403, $e->getCode());
        }

        $metadataArray = $readMetadataApi->listBucketMetadata($bucketId);
        $this->assertCount(1, $metadataArray);

        // update
        try {
            $readMetadataApi->postBucketMetadata(
                $bucketId,
                $provider,
                [
                    [
                        'key' => self::TEST_METADATA_KEY_1,
                        'value' => 'changed',
                    ],
                ]
            );

            $this->fail('Token with read permissions should not update metadata');
        } catch (ClientException $e) {
            $this->assertEquals(403, $e->getCode());
        }

        $metadataArray = $readMetadataApi->listBucketMetadata($bucketId);
        $this->assertCount(1, $metadataArray);

        $metadata = $metadataArray[0];
        $this->assertEquals($testMetadataValue, $metadata['value']);

        // new metadata
        try {
            $readMetadataApi->postBucketMetadata(
                $bucketId,
                $provider,
                [
                    [
                        'key' => self::TEST_METADATA_KEY_2,
                        'value' => $testMetadataValue,
                    ],
                ]
            );

            $this->fail('Token with read permissions should not create metadata');
        } catch (ClientException $e) {
            $this->assertEquals(403, $e->getCode());
        }

        $metadataArray = $readMetadataApi->listBucketMetadata($bucketId);
        $this->assertCount(1, $metadataArray);
    }

    public function apiEndpoints(): Generator
    {
        foreach ((new TestSetupHelper())->provideComponentsClientTypeBasedOnSuite($this) as $key => $providedValue) {
            $tableId = '.table';
            $columnId = $tableId . '.id';
            yield $key . ' columnEndpoint' => [...$providedValue, self::ENDPOINT_TYPE_COLUMNS, $columnId];
            yield $key . ' tableEndpoint' => [...$providedValue, self::ENDPOINT_TYPE_TABLES, $tableId];
            yield $key . ' bucketEndpoint' => [...$providedValue, self::ENDPOINT_TYPE_BUCKETS, ''];
        }
    }

    private function postMetadata($apiEndpoint, $objId, $metadata)
    {
        $metadataApi = new Metadata($this->_testClient);
        switch ($apiEndpoint) {
            case self::ENDPOINT_TYPE_COLUMNS:
                $res = $metadataApi->postColumnMetadata($objId, self::TEST_PROVIDER, $metadata);
                break;
            case self::ENDPOINT_TYPE_TABLES:
                $res = $metadataApi->postTableMetadata($objId, self::TEST_PROVIDER, $metadata);
                break;
            case self::ENDPOINT_TYPE_BUCKETS:
                $res = $metadataApi->postBucketMetadata($objId, self::TEST_PROVIDER, $metadata);
                break;
        }
    }

    private function deleteMetadata($apiEndpoint, $objId, $metadataId)
    {
        $metadataApi = new Metadata($this->_testClient);
        switch ($apiEndpoint) {
            case self::ENDPOINT_TYPE_COLUMNS:
                $metadataApi->deleteColumnMetadata($objId, $metadataId);
                break;
            case self::ENDPOINT_TYPE_TABLES:
                $metadataApi->deleteTableMetadata($objId, $metadataId);
                break;
            case self::ENDPOINT_TYPE_BUCKETS:
                $metadataApi->deleteBucketMetadata($objId, $metadataId);
                break;
        }
    }

    private function prepareTokenWithReadPrivilegeForBucket($bucketId, string $role)
    {
        $options = new TokenCreateOptions();
        $options
            ->setExpiresIn(60 * 5)
            ->setDescription(sprintf('Test read of "%s" bucket', $bucketId))
            ->addBucketPermission($bucketId, TokenAbstractOptions::BUCKET_PERMISSION_READ);
        if ($role === TestSetupHelper::ROLE_PROD_MANAGER) {
            $options->setCanCreateJobs(true);
        }
        $token = $this->tokens->createToken($options);
        return $token['token'];
    }

    /**
     * @param string $tableId
     * @return string
     */
    private function getMetadataTestTableId($tableId)
    {
        return sprintf('%s.%s', $this->getTestBucketId(), $tableId);
    }

    /**
     * @param string $tableId
     * @param string $columnId
     * @return string
     */
    private function getMetadataTestColumnId($tableId, $columnId)
    {
        return sprintf('%s.%s.%s', $this->getTestBucketId(), $tableId, $columnId);
    }

     /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     * @group SOX-66
     */
    public function testTypedTableMetadataRestrictions(string $devBranchType, string $userRole): void
    {
        $this->skipTestForBackend([
            self::BACKEND_REDSHIFT,
        ], 'Redshift backend does not have typed tables.');

        $normalTableName = 'test_restrictions_normal';
        $normalTableId = $this->_testClient->createTableAsync(
            $this->getTestBucketId(),
            $normalTableName,
            new CsvFile(__DIR__ . '/../_data/languages.csv')
        );

        $typedTableName = 'test_restrictions';
        $typedTableId = $this->_testClient->createTableDefinition($this->getTestBucketId(), [
            'name' => $typedTableName,
            'primaryKeysNames' => [],
            'columns' => [
                [
                    'name' => 'name',
                    'basetype' => 'STRING',
                ],
            ],
        ]);

        $md = [
            [
                'key' => self::TEST_METADATA_KEY_1,
                'value' => 'testval',
            ],
        ];

        $metadataApi = new Metadata($this->_testClient);

        // test that can be set for normal tables
        $metadata = $metadataApi->postTableMetadata($normalTableId, 'storage', $md);
        $this->assertSame('storage', $metadata[0]['provider']);

        try {
            $metadataApi->postTableMetadata($typedTableId, 'storage', $md);
            $this->fail('Metadata with "storage" provider cannot be created by user.');
        } catch (ClientException $e) {
            $this->assertSame(403, $e->getCode());
            $this->assertSame('Metadata with "storage" provider cannot be edited by user.', $e->getMessage());
            $this->assertSame('storage.metadata.invalidProvider', $e->getStringCode());
        }

        // get current storage metadata
        $items = $metadataApi->listTableMetadata($typedTableId);
        $storageMetadata = array_filter($items, static function ($item) {
            return $item['provider'] === 'storage';
        });

        try {
            $metadataApi->deleteTableMetadata($typedTableId, $storageMetadata[0]['id']);
            $this->fail('Metadata with "storage" provider cannot be deleted by user.');
        } catch (ClientException $e) {
            $this->assertSame(403, $e->getCode());
            $this->assertSame('Metadata with "storage" provider cannot be deleted by user.', $e->getMessage());
            $this->assertSame('storage.metadata.invalidProvider', $e->getStringCode());
        }

        // test that can be set for normal table columns
        $columnId = $this->getMetadataTestColumnId($normalTableName, 'name');
        $metadata = $metadataApi->postColumnMetadata($columnId, 'storage', $md);
        $this->assertSame('storage', $metadata[0]['provider']);

        $columnId = $this->getMetadataTestColumnId($typedTableName, 'name');
        try {
            $metadataApi->postColumnMetadata(
                $columnId,
                'storage',
                $md
            );
            $this->fail('Metadata with "storage" provider cannot be created by user.');
        } catch (ClientException $e) {
            $this->assertSame(403, $e->getCode());
            $this->assertSame('Metadata with "storage" provider cannot be edited by user.', $e->getMessage());
            $this->assertSame('storage.metadata.invalidProvider', $e->getStringCode());
        }

        $items = $metadataApi->listColumnMetadata($columnId);
        $storageMetadata = array_filter($items, static function ($item) {
            return $item['provider'] === 'storage';
        });
        try {
            $metadataApi->deleteColumnMetadata(
                $columnId,
                $storageMetadata[0]['id'],
            );
            $this->fail('Metadata with "storage" provider cannot be created by user.');
        } catch (ClientException $e) {
            $this->assertSame(403, $e->getCode());
            $this->assertSame('Metadata with "storage" provider cannot be deleted by user.', $e->getMessage());
            $this->assertSame('storage.metadata.invalidProvider', $e->getStringCode());
        }
    }

    public function provideComponentsClientTypeBasedOnSuite(): ?array
    {
        return (new TestSetupHelper())->provideComponentsClientTypeBasedOnSuite($this);
    }

    private function getReadClient(string $devBranchType, string $bucketId, string $role): Client
    {
        if (in_array($role, TestSetupHelper::PROTECTED_DEFAULT_BRANCH_ROLES)) {
            $this->tokens = new Tokens($this->getClientBasedOnRole($role));
        }

        if ($devBranchType === ClientProvider::DEV_BRANCH) {
            assert($this->_testClient instanceof BranchAwareClient);
            return $this->getBranchAwareClient($this->_testClient->getCurrentBranchId(), [
                'token' => $this->prepareTokenWithReadPrivilegeForBucket($bucketId, $role),
                'url' => STORAGE_API_URL,
                'backoffMaxTries' => 1,
            ]);
        }
        return $this->getClient([
            'token' => $this->prepareTokenWithReadPrivilegeForBucket($bucketId, $role),
            'url' => STORAGE_API_URL,
            'backoffMaxTries' => 1,
        ]);
    }
}
