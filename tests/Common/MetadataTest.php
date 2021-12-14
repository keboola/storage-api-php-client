<?php

namespace Keboola\Test\Common;

use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Options\TokenAbstractOptions;
use Keboola\StorageApi\Options\TokenCreateOptions;
use Keboola\Test\StorageApiTestCase;
use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Metadata;

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

    public function setUp()
    {
        parent::setUp();
//        $this->_initEmptyTestBuckets();
//        $metadataApi = new Metadata($this->_client);
//        $metadatas = $metadataApi->listBucketMetadata($this->getTestBucketId());
//        foreach ($metadatas as $md) {
//            $metadataApi->deleteBucketMetadata($this->getTestBucketId(), $md['id']);
//        }
//        $this->_client->createTable($this->getTestBucketId(), "table", new CsvFile(__DIR__ . '/../_data/users.csv'));
    }

    public function testBucketMetadata()
    {
        $bucketId = $this->getTestBucketId();
        $metadataApi = new Metadata($this->_client);

        $md = [
            'key' => self::TEST_METADATA_KEY_1,
            'value' => 'testval',
        ];
        $md2 = [
            'key' => self::TEST_METADATA_KEY_2,
            'value' => 'testval',
        ];
        $testMetadata = array($md, $md2);

        $provider = self::TEST_PROVIDER;
        $metadatas = $metadataApi->postBucketMetadata($bucketId, $provider, $testMetadata);

        $this->assertEquals(2, count($metadatas));
        $this->assertArrayHasKey("key", $metadatas[0]);
        $this->assertArrayHasKey("value", $metadatas[0]);
        $this->assertArrayHasKey("provider", $metadatas[0]);
        $this->assertArrayHasKey("timestamp", $metadatas[0]);
        $this->assertRegExp(self::ISO8601_REGEXP, $metadatas[0]['timestamp']);
        $this->assertEquals(self::TEST_PROVIDER, $metadatas[0]['provider']);

        $origValue = $metadatas[0]['value'];
        $mdCopy = $metadatas[0];
        $mdCopy['value'] = "newValue";

        $newMetadata = $metadataApi->postBucketMetadata($bucketId, $provider, array($mdCopy));

        foreach ($newMetadata as $metadata) {
            if ($metadata['id'] == $metadatas[0]['id']) {
                $this->assertEquals("newValue", $metadata['value']);
            } else {
                $this->assertEquals("testval", $metadata['value']);
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

    public function testPrepareMetadataForTest()
    {
        $inTestBucketId = 'in.c-API-tests';//$this->getTestBucketId();
        $inBucketTableId = $inTestBucketId . '.table';
        $inBucketColumnId = $inBucketTableId . '.id';
        $inBucketColumnName = $inBucketTableId . '.name';

        $metadataApi = new Metadata($this->_client);

        // prepare metadata for bucket
        $metadataApi->postBucketMetadata(
            $inTestBucketId,
            'wr-db',
            [
                [
                    'key' => 'KBC.metadata',
                    'value' => 'wr-db',
                ],
            ]
        );

        $metadataApi->postBucketMetadata(
            $inTestBucketId,
            'user',
            [
                [
                    'key' => 'KBC.metadata',
                    'value' => 'bucket-user-metadata',
                ],
            ]
        );

        // prepare metadata for table
        $metadataApi->postTableMetadata(
            $inBucketTableId,
            'wr-db',
            [
                [
                    'key' => 'KBC.metadata',
                    'value' => 'wr-db',
                ],
            ]
        );

        $metadataApi->postTableMetadata(
            $inBucketTableId,
            'user',
            [
                [
                    'key' => 'KBC.metadata',
                    'value' => 'user-metadata',
                ],
            ]
        );

        // prepare metadata for column
        $testMetadata = [
            [
                'key' => 'KBC.datatype.basetype',
                'value' => 'STRING',
            ],
            [
                'key' => 'KBC.datatype.length',
                'value' => '255',
            ],
        ];

        $metadataApi->postColumnMetadata(
            $inBucketColumnId,
            'wr-db',
            $testMetadata
        );

        $metadataApi->postColumnMetadata(
            $inBucketColumnName,
            'wr-db',
            $testMetadata
        );

        $testMetadata = [
            [
                'key' => 'KBC.datatype.basetype',
                'value' => 'INTEGER',
            ],
            [
                'key' => 'KBC.datatype.length',
                'value' => '16',
            ],
        ];

        $metadataApi->postColumnMetadata(
            $inBucketColumnId,
            'user',
            $testMetadata
        );

        $testMetadata = [
            [
                'key' => 'KBC.datatype.basetype',
                'value' => 'STRING',
            ],
            [
                'key' => 'KBC.datatype.length',
                'value' => '26',
            ],
        ];

        $metadataApi->postColumnMetadata(
            $inBucketColumnName,
            'user',
            $testMetadata
        );

        $this->expectNotToPerformAssertions();
    }

    public function testPartialUpdateMetadata()
    {
        $inTestBucketId = 'in.c-API-tests';//$this->getTestBucketId();
        $inBucketTableId = $inTestBucketId . '.table';
        $inBucketColumnId = $inBucketTableId . '.id';

        $metadataApi = new Metadata($this->_client);

        $metadataApi->postColumnMetadata(
            $inBucketColumnId,
            'user',
            [
                [
                    'key' => 'KBC.datatype.length',
                    'value' => '26',
                ],
            ]
        );
    }

    public function testUpdateWrDbMetadataShouldCorruptUserMetadata()
    {
        // before this test please simulate bug
        // comment in connection repo \Model_Metadata::generateUpdateQuery line 162
        // and take a note time when run this test
        $inTestBucketId = 'in.c-API-tests';
        $inBucketTableId = $inTestBucketId . '.table';
        $inBucketColumnId = $inBucketTableId . '.id';
        $inBucketColumnName = $inBucketTableId . '.name';

        $metadataApi = new Metadata($this->_client);

        $testMetadata = [
            [
                'key' => 'KBC.datatype.basetype',
                'value' => 'STRING',
            ],
            [
                'key' => 'KBC.datatype.length',
                'value' => '255',
            ],
        ];

        $metadataApi->postColumnMetadata(
            $inBucketColumnId,
            'wr-db',
            [
                [
                    'key' => 'KBC.datatype.basetype',
                    'value' => 'STRING',
                ],
            ]
        );

        $metadataApi->postColumnMetadata(
            $inBucketColumnId,
            'wr-db',
            [
                [
                    'key' => 'KBC.datatype.length',
                    'value' => '255',
                ],
            ]
        );

        $metadataApi->postColumnMetadata(
            $inBucketColumnName,
            'wr-db',
            $testMetadata
        );

        $metadataApi->postTableMetadata(
            $inBucketTableId,
            'wr-db',
            [
                [
                    'key' => 'KBC.metadata',
                    'value' => 'wr-db-metadata',
                ],
            ]
        );

        $metadataApi->postBucketMetadata(
            $inTestBucketId,
            'wr-db',
            [
                [
                    'key' => 'KBC.metadata',
                    'value' => 'wr-db-metadata',
                ],
            ]
        );

        $corruptedColumnMetadata = [
            [
                'key' => "KBC.datatype.basetype",
                'value' => "STRING",
                'provider' => "wr-db",
            ],
            [
                'key' => 'KBC.datatype.length',
                'value' => '255',
                'provider' => "wr-db",
            ],
            [
                'key' => "KBC.datatype.basetype",
                'value' => "STRING",
                'provider' => "user",
            ],
            [
                'key' => 'KBC.datatype.length',
                'value' => '255',
                'provider' => "user",
            ],
        ];

        // test is user metadata corrupted
        $columnIdMetadata = $metadataApi->listColumnMetadata($inBucketColumnId);
        $this->assertMetadataEquals($corruptedColumnMetadata[0], $columnIdMetadata[0]);
        $this->assertMetadataEquals($corruptedColumnMetadata[1], $columnIdMetadata[1]);
        $this->assertMetadataEquals($corruptedColumnMetadata[2], $columnIdMetadata[2]);
        $this->assertMetadataEquals($corruptedColumnMetadata[3], $columnIdMetadata[3]);

        $columnNameMetadata = $metadataApi->listColumnMetadata($inBucketColumnName);
        $this->assertMetadataEquals($corruptedColumnMetadata[0], $columnNameMetadata[0]);
        $this->assertMetadataEquals($corruptedColumnMetadata[1], $columnNameMetadata[1]);
        $this->assertMetadataEquals($corruptedColumnMetadata[2], $columnNameMetadata[2]);
        $this->assertMetadataEquals($corruptedColumnMetadata[3], $columnNameMetadata[3]);

        $corruptedMetadata = [
            [
                'key' => "KBC.metadata",
                'value' => "wr-db-metadata",
                'provider' => "wr-db",
            ],
            [
                'key' => "KBC.metadata",
                'value' => "wr-db-metadata",
                'provider' => "user",
            ],
        ];
        $tableMetadata = $metadataApi->listTableMetadata($inBucketTableId);
        $this->assertMetadataEquals($corruptedMetadata[0], $tableMetadata[0]);
        $this->assertMetadataEquals($corruptedMetadata[1], $tableMetadata[1]);

        $bucketMetadata = $metadataApi->listBucketMetadata($inTestBucketId);
        $this->assertMetadataEquals($corruptedMetadata[0], $bucketMetadata[0]);
        $this->assertMetadataEquals($corruptedMetadata[1], $bucketMetadata[1]);
    }

    public function testBackFilledMetadata()
    {
        $inTestBucketId = 'in.c-API-tests';
        $inBucketTableId = $inTestBucketId . '.table';
        $inBucketColumnId = $inBucketTableId . '.id';
        $inBucketColumnName = $inBucketTableId . '.name';

        $metadataApi = new Metadata($this->_client);

        $fixedColumnIdMetadata = [
            [
                'key' => "KBC.datatype.basetype",
                'value' => "STRING",
                'provider' => "wr-db",
            ],
            [
                'key' => 'KBC.datatype.length',
                'value' => '255',
                'provider' => "wr-db",
            ],
            [
                'key' => "KBC.datatype.basetype",
                'value' => "INTEGER",
                'provider' => "user",
            ],
            [
                'key' => 'KBC.datatype.length',
                'value' => '16',
                'provider' => "user",
            ],
        ];

        $columnIdMetadata = $metadataApi->listColumnMetadata($inBucketColumnId);
        $this->assertMetadataEquals($fixedColumnIdMetadata[0], $columnIdMetadata[0]);
        $this->assertMetadataEquals($fixedColumnIdMetadata[1], $columnIdMetadata[1]);
        $this->assertMetadataEquals($fixedColumnIdMetadata[2], $columnIdMetadata[2]);
        $this->assertMetadataEquals($fixedColumnIdMetadata[3], $columnIdMetadata[3]);

        $fixedColumnNameMetadata = [
            [
                'key' => "KBC.datatype.basetype",
                'value' => "STRING",
                'provider' => "wr-db",
            ],
            [
                'key' => 'KBC.datatype.length',
                'value' => '255',
                'provider' => "wr-db",
            ],
            [
                'key' => "KBC.datatype.basetype",
                'value' => "STRING",
                'provider' => "user",
            ],
            [
                'key' => 'KBC.datatype.length',
                'value' => '26',
                'provider' => "user",
            ],
        ];

        $columnNameMetadata = $metadataApi->listColumnMetadata($inBucketColumnName);
        $this->assertMetadataEquals($fixedColumnNameMetadata[0], $columnNameMetadata[0]);
        $this->assertMetadataEquals($fixedColumnNameMetadata[1], $columnNameMetadata[1]);
        $this->assertMetadataEquals($fixedColumnNameMetadata[2], $columnNameMetadata[2]);
        $this->assertMetadataEquals($fixedColumnNameMetadata[3], $columnNameMetadata[3]);

        $fixedBucketMetadata = [
            [
                'key' => "KBC.metadata",
                'value' => "wr-db-metadata",
                'provider' => "wr-db",
            ],
            [
                'key' => "KBC.metadata",
                'value' => "bucket-user-metadata",
                'provider' => "user",
            ],
        ];

        $bucketMetadata = $metadataApi->listBucketMetadata($inTestBucketId);
        $this->assertMetadataEquals($fixedBucketMetadata[0], $bucketMetadata[0]);
        $this->assertMetadataEquals($fixedBucketMetadata[1], $bucketMetadata[1]);

        $fixedTableMetadata = [
            [
                'key' => "KBC.metadata",
                'value' => "wr-db-metadata",
                'provider' => "wr-db",
            ],
            [
                'key' => "KBC.metadata",
                'value' => "user-metadata",
                'provider' => "user",
            ],
        ];

        $tableMetadata = $metadataApi->listTableMetadata($inBucketTableId);
        $this->assertMetadataEquals($fixedTableMetadata[0], $tableMetadata[0]);
        $this->assertMetadataEquals($fixedTableMetadata[1], $tableMetadata[1]);
    }

    private function assertMetadataEquals(array $expected, array $actual)
    {
        foreach ($expected as $key => $value) {
            self::assertArrayHasKey($key, $actual);
            self::assertSame($value, $actual[$key]);
        }
        self::assertArrayHasKey('timestamp', $actual);
    }

    public function testColumnMetadataOverwrite()
    {
        $outTestBucketId = $this->getTestBucketId(self::STAGE_OUT);
        $outBucketTableId = $this->_client->createTable(
            $outTestBucketId,
            "table",
            new CsvFile(__DIR__ . '/../_data/users.csv')
        );

        $outBucketColumnId = $outBucketTableId . '.id';
        $metadataApi = new Metadata($this->_client);

        $testMetadata = [
            [
                'key' => 'KBC.datatype.nullable',
                'value' => 'testValue',
            ]
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

        $columnId = $this->getTestBucketId() . '.table.id';
        $metadataApi = new Metadata($this->_client);

        $testMetadata = [
            [
                'key' => 'KBC.datatype.nullable',
                'value' => 'true',
            ]
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
            ]
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

    public function testTableMetadata()
    {
        $tableId = $this->getTestBucketId() . '.table';
        $metadataApi = new Metadata($this->_client);

        $md = [
            'key' => self::TEST_METADATA_KEY_1,
            'value' => 'testval',
        ];
        $md2 = [
            'key' => self::TEST_METADATA_KEY_2,
            'value' => 'testval',
        ];
        $testMetadata = array($md, $md2);

        $provider = self::TEST_PROVIDER;

        $metadatas = $metadataApi->postTableMetadata($tableId, $provider, $testMetadata);

        $this->assertEquals(2, count($metadatas));
        $this->assertArrayHasKey("key", $metadatas[0]);
        $this->assertArrayHasKey("value", $metadatas[0]);
        $this->assertArrayHasKey("provider", $metadatas[0]);
        $this->assertArrayHasKey("timestamp", $metadatas[0]);
        $this->assertRegExp(self::ISO8601_REGEXP, $metadatas[0]['timestamp']);
        $this->assertEquals(self::TEST_PROVIDER, $metadatas[0]['provider']);

        $mdCopy = $metadatas[0];
        $mdCopy['value'] = "newValue";

        $newMetadata = $metadataApi->postTableMetadata($tableId, $provider, array($mdCopy));

        foreach ($newMetadata as $metadata) {
            if ($metadata['id'] == $metadatas[0]['id']) {
                $this->assertEquals("newValue", $metadata['value']);
                $this->assertGreaterThanOrEqual(
                    strtotime($metadatas[0]['timestamp']),
                    strtotime($metadata['timestamp'])
                );
            } else {
                $this->assertEquals("testval", $metadata['value']);
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
        $bucketMetadata = array($md);
        $metadataApi->postBucketMetadata($this->getTestBucketId(), $provider, $bucketMetadata);

        $table = $this->_client->getTable($tableId);
        $this->assertArrayHasKey('metadata', $table['bucket']);
        $this->assertCount(1, $table['bucket']['metadata']);
        $this->assertEquals($table['bucket']['metadata'][0]['key'], $md['key']);
        $this->assertEquals($table['bucket']['metadata'][0]['value'], $md['value']);
    }


    public function testTableMetadataForTokenWithReadPrivilege()
    {
        $testMetadataValue = 'testval';

        $bucketId = $this->getTestBucketId();
        $tableId = $this->getTestBucketId() . '.table';
        $metadataApi = new Metadata($this->_client);

        $provider = self::TEST_PROVIDER;
        $metadataApi->postTableMetadata(
            $tableId,
            $provider,
            [
                [
                    'key' => self::TEST_METADATA_KEY_1,
                    'value' => $testMetadataValue,
                ]
            ]
        );


        $readClient = $this->getClient([
            'token' => $this->prepareTokenWithReadPrivilegeForBucket($bucketId),
            'url' => STORAGE_API_URL,
            'backoffMaxTries' => 1,
        ]);

        $readMetadataApi = new Metadata($readClient);

        $metadataArray = $readMetadataApi->listTableMetadata($tableId);
        $this->assertCount(1, $metadataArray);

        $metadata = reset($metadataArray);
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
                    ]
                ]
            );

            $this->fail('Token with read permissions should not update metadata');
        } catch (ClientException $e) {
            $this->assertEquals(403, $e->getCode());
        }

        $metadataArray = $readMetadataApi->listTableMetadata($tableId);
        $this->assertCount(1, $metadataArray);

        $metadata = reset($metadataArray);
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
                    ]
                ]
            );

            $this->fail('Token with read permissions should not create metadata');
        } catch (ClientException $e) {
            $this->assertEquals(403, $e->getCode());
        }

        $metadataArray = $readMetadataApi->listTableMetadata($tableId);
        $this->assertCount(1, $metadataArray);
    }

    public function testTableDeleteWithMetadata()
    {
        $tableId = $this->getTestBucketId() . '.table';
        $columnId = $this->getTestBucketId() . '.table.sex';
        $metadataApi = new Metadata($this->_client);

        $md = [
            'key' => self::TEST_METADATA_KEY_1,
            'value' => 'testval',
        ];
        $md2 = [
            'key' => self::TEST_METADATA_KEY_2,
            'value' => 'testval',
        ];
        $testMetadata = array($md, $md2);

        $provider = self::TEST_PROVIDER;

        $metadataApi->postTableMetadata($tableId, $provider, $testMetadata);
        $tableDetail = $this->_client->getTable($tableId);

        $this->assertNotEmpty($tableDetail['metadata']);
        $this->assertCount(2, $tableDetail['metadata']);

        $metadataApi->postColumnMetadata($columnId, $provider, $testMetadata);
        $tableDetail = $this->_client->getTable($tableId);

        $this->assertNotEmpty($tableDetail['columnMetadata']);
        $this->assertCount(2, $tableDetail['columnMetadata']['sex']);

        $this->_client->dropTable($tableId);

        $this->expectException(ClientException::class);
        $this->expectExceptionCode(404);
        $metadataApi->listTableMetadata($tableId);

        $this->expectException(ClientException::class);
        $this->expectExceptionCode(404);
        $metadataApi->listTableMetadata($columnId);
    }

    public function testColumnMetadata()
    {
        $columnId = $this->getTestBucketId() . '.table.id';
        $metadataApi = new Metadata($this->_client);

        $md = [
            'key' => self::TEST_METADATA_KEY_1,
            'value' => 'testval',
        ];
        $md2 = [
            'key' => self::TEST_METADATA_KEY_2,
            'value' => 'testval',
        ];
        $testMetadata = array($md, $md2);

        $provider = self::TEST_PROVIDER;

        $metadatas = $metadataApi->postColumnMetadata($columnId, $provider, $testMetadata);

        $this->assertEquals(2, count($metadatas));
        $this->assertArrayHasKey("key", $metadatas[0]);
        $this->assertArrayHasKey("value", $metadatas[0]);
        $this->assertArrayHasKey("provider", $metadatas[0]);
        $this->assertArrayHasKey("timestamp", $metadatas[0]);
        $this->assertRegExp(self::ISO8601_REGEXP, $metadatas[0]['timestamp']);
        $this->assertEquals(self::TEST_PROVIDER, $metadatas[0]['provider']);

        $mdCopy = $metadatas[0];
        $mdCopy['value'] = "newValue";

        $newMetadata = $metadataApi->postColumnMetadata($columnId, $provider, array($mdCopy));
        foreach ($newMetadata as $metadata) {
            if ($metadata['id'] == $metadatas[0]['id']) {
                $this->assertEquals("newValue", $metadata['value']);
                $this->assertGreaterThanOrEqual(
                    strtotime($metadatas[0]['timestamp']),
                    strtotime($metadata['timestamp'])
                );
            } else {
                $this->assertEquals("testval", $metadata['value']);
            }
        }

        $metadataApi->deleteColumnMetadata($columnId, $mdCopy['id']);

        $mdList = $metadataApi->listColumnMetadata($columnId);

        $this->assertEquals(1, count($mdList));

        $this->assertEquals($metadatas[1]['key'], $mdList[0]['key']);
        $this->assertEquals($metadatas[1]['value'], $mdList[0]['value']);
        $this->assertEquals($metadatas[1]['provider'], $mdList[0]['provider']);
        $this->assertEquals($metadatas[1]['timestamp'], $mdList[0]['timestamp']);

        // create alias of alias
        $this->_client->createAliasTable(
            $this->getTestBucketId(),
            $this->getTestBucketId() . '.table',
            'tableAlias'
        );
        $this->_client->createAliasTable(
            $this->getTestBucketId(),
            $this->getTestBucketId() . '.tableAlias',
            'tableAliasAlias'
        );

        // test list tables call
        $tables = $this->_client->listTables(null, ['include' => 'columnMetadata']);
        // call return all tables, filter the alias of alias one

        $aliasAliasTableId = $this->getTestBucketId() . '.tableAliasAlias';
        $tables = array_values(array_filter($tables, function ($table) use ($aliasAliasTableId) {
            return $table['id'] === $aliasAliasTableId;
        }));

        // the metadata should be propagated from the source table
        $this->assertNotEmpty($tables[0]['sourceTable']['columnMetadata']['id']);
        $this->assertEquals(
            $mdList,
            $tables[0]['sourceTable']['columnMetadata']['id']
        );

        $alias = $this->_client->getTable($aliasAliasTableId);
        $this->assertNotEmpty($alias['sourceTable']['columnMetadata']);
        $this->assertEquals(
            $mdList,
            $alias['sourceTable']['columnMetadata']['id']
        );

        $tables = $this->_client->listTables($this->getTestBucketId(), ['include' => 'columnMetadata']);
        $tables = array_values(array_filter($tables, function ($table) use ($aliasAliasTableId) {
            return $table['id'] === $aliasAliasTableId;
        }));
        $this->assertEquals(
            $mdList,
            $tables[0]['sourceTable']['columnMetadata']['id']
        );
    }

    public function testColumnMetadataForTokenWithReadPrivilege()
    {
        $testMetadataValue = 'testval';

        $bucketId = $this->getTestBucketId();
        $columnId = $bucketId . '.table.id';
        $metadataApi = new Metadata($this->_client);

        $provider = self::TEST_PROVIDER;
        $metadataApi->postColumnMetadata(
            $columnId,
            $provider,
            [
                [
                    'key' => self::TEST_METADATA_KEY_1,
                    'value' => $testMetadataValue,
                ]
            ]
        );


        $readClient = $this->getClient([
            'token' => $this->prepareTokenWithReadPrivilegeForBucket($bucketId),
            'url' => STORAGE_API_URL,
            'backoffMaxTries' => 1,
        ]);

        $readMetadataApi = new Metadata($readClient);

        $metadataArray = $readMetadataApi->listColumnMetadata($columnId);
        $this->assertCount(1, $metadataArray);

        $metadata = reset($metadataArray);
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
                    ]
                ]
            );

            $this->fail('Token with read permissions should not update metadata');
        } catch (ClientException $e) {
            $this->assertEquals(403, $e->getCode());
        }

        $metadataArray = $readMetadataApi->listColumnMetadata($columnId);
        $this->assertCount(1, $metadataArray);

        $metadata = reset($metadataArray);
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
                    ]
                ]
            );

            $this->fail('Token with read permissions should not create metadata');
        } catch (ClientException $e) {
            $this->assertEquals(403, $e->getCode());
        }

        $metadataArray = $readMetadataApi->listColumnMetadata($columnId);
        $this->assertCount(1, $metadataArray);
    }

    public function testTableColumnDeleteWithMetadata()
    {
        $tableId = $this->getTestBucketId() . '.table';
        $columnId = $this->getTestBucketId() . '.table.sex';
        $metadataApi = new Metadata($this->_client);

        $md = [
            'key' => self::TEST_METADATA_KEY_1,
            'value' => 'testval',
        ];
        $md2 = [
            'key' => self::TEST_METADATA_KEY_2,
            'value' => 'testval',
        ];
        $testMetadata = array($md, $md2);

        $provider = self::TEST_PROVIDER;

        $metadataApi->postColumnMetadata($columnId, $provider, $testMetadata);

        $tableDetail = $this->_client->getTable($tableId);

        $this->assertNotEmpty($tableDetail['columnMetadata']);
        $this->assertEquals(2, count($tableDetail['columnMetadata']['sex']));

        $this->_client->deleteTableColumn($tableId, 'sex');

        $tableDetail = $this->_client->getTable($tableId);
        $this->assertEmpty($tableDetail['columnMetadata']);

        $this->assertEquals(array('id','name','city'), $tableDetail['columns']);


        $this->expectException(ClientException::class);
        $this->expectExceptionCode(404);

        $metadataApi->listColumnMetadata($columnId);
    }


    public function testUpdateTimestamp()
    {
        $bucketId = $this->getTestBucketId();
        $metadataApi = new Metadata($this->_client);

        $md = [
            'key' => self::TEST_METADATA_KEY_1,
            'value' => 'testval',
        ];
        $md2 = [
            'key' => self::TEST_METADATA_KEY_1,
            'value' => 'new testval',
        ];
        $testMetadata = array($md);

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
     */
    public function testInvalidMetadata($apiEndpoint, $object)
    {
        $bucketId = self::getTestBucketId();
        $object = ($apiEndpoint === self::ENDPOINT_TYPE_BUCKETS) ? $bucketId : $bucketId . $object;

        $md = [
            "key" => "%invalidKey", // invalid char %
            "value" => "testval",
        ];

        try {
            // this should fail because metadata objects must be provided in an array
            $this->postMetadata($apiEndpoint, $object, $md);
            $this->fail("metadata must be an array of key-value objects.");
        } catch (ClientException $e) {
            $this->assertEquals("storage.metadata.invalidStructure", $e->getStringCode());
            $this->assertEquals(
                "Invalid structure. Metadata must be provided as an array of objects with 'key' and 'value' members",
                $e->getMessage()
            );
        }

        try {
            $this->postMetadata($apiEndpoint, $object, [$md]);
            $this->fail("Should throw invalid key exception");
        } catch (ClientException $e) {
            $this->assertEquals("storage.metadata.invalidKey", $e->getStringCode());
            $this->assertEquals("Invalid Metadata Key (%invalidKey)", $e->getMessage());
        }

        // length > 255
        $invalidKey = str_pad("validKey", 260, "+");
        $md = [
            "key" => $invalidKey,
            "value" => "testval",
        ];
        try {
            $this->postMetadata($apiEndpoint, $object, [$md]);
            $this->fail("Should throw invalid key exception");
        } catch (ClientException $e) {
            $this->assertEquals("storage.metadata.invalidKey", $e->getStringCode());
            $this->assertEquals("Invalid Metadata Key ({$invalidKey})", $e->getMessage());
        }

        $md = [
            "key" => "", // empty key
            "value" => "testval",
        ];
        try {
            $this->postMetadata($apiEndpoint, $object, [$md]);
            $this->fail("Should throw invalid key exception");
        } catch (ClientException $e) {
            $this->assertEquals("storage.metadata.invalidKey", $e->getStringCode());
            $this->assertEquals("Invalid Metadata Key ()", $e->getMessage());
        }
    }

    /**
     * $apiEndpoint represents part of URl because it has to call apiPost() directly without postMetadata() because
     * postMetadata() checks input in on client side, but this test should call it with wrong data in order to test it
     * in connection
     *
     * @dataProvider apiEndpoints
     */
    public function testInvalidMetadataWhenMetadataIsNotArray($apiEndpoint, $object)
    {
        $bucketId = self::getTestBucketId();
        $objectId = $bucketId . $object;

        try {
            $this->_client->apiPost("{$apiEndpoint}/{$objectId}/metadata", [
                "provider" => 'valid',
                "metadata" => 'not an array',
            ]);
            $this->fail("Should throw invalid key exception");
        } catch (ClientException $e) {
            $this->assertEquals("storage.metadata.invalidParameter", $e->getStringCode());
            $this->assertEquals("The metadata parameter must be an array.", $e->getMessage());
        }
    }

    /**
     * @dataProvider apiEndpoints
     */
    public function testInvalidMetadataWhenMetadataIsNotPresent($apiEndpoint, $object)
    {
        $bucketId = self::getTestBucketId();
        $objectId = $bucketId . $object;

        try {
            $this->_client->apiPost("{$apiEndpoint}/{$objectId}/metadata", [
                "provider" => 'valid',
            ]);
            $this->fail("Should throw invalid key exception");
        } catch (ClientException $e) {
            $this->assertEquals("storage.metadata.invalidParameter", $e->getStringCode());
            $this->assertEquals("The metadata parameter must be an array.", $e->getMessage());
        }
    }

    /**
     * @dataProvider apiEndpoints
     */
    public function testMetadata40xs($apiEndpoint, $object)
    {
        $bucketId = self::getTestBucketId();
        $object = ($apiEndpoint === "bucket") ? $bucketId : $bucketId . $object;

        // test invalid number
        try {
            $this->deleteMetadata($apiEndpoint, $object, 9999999);
            $this->fail("Invalid metadataId");
        } catch (ClientException $e) {
            $this->assertEquals(404, $e->getCode());
            $this->assertEquals('storage.metadata.notFound', $e->getStringCode());
            $this->assertEquals('The supplied metadata ID was not found', $e->getMessage());
        }

        // test null
        try {
            $this->deleteMetadata($apiEndpoint, $object, null);
            $this->fail("Invalid metadataId");
        } catch (ClientException $e) {
            $this->assertEquals(400, $e->getCode());
            $this->assertEquals('APPLICATION_ERROR', $e->getStringCode());
            $this->assertEquals('exceptions.storage.metadata.invalidDelete', $e->getMessage());
        }

        // not numeric value
        try {
            $this->deleteMetadata($apiEndpoint, $object, 'notNumber');
            $this->fail("Invalid metadataId");
        } catch (ClientException $e) {
            $this->assertEquals(400, $e->getCode());
            $this->assertEquals('APPLICATION_ERROR', $e->getStringCode());
            $this->assertEquals(
                'Argument "subResource" is expected to be type "int", value "notNumber" given.',
                $e->getMessage()
            );
        }
    }

    public function testInvalidProvider()
    {
        $metadataApi = new Metadata($this->_client);
        $this->getTestBucketId() . '.table';
        $md = [
            "key" => "validKey",
            "value" => "testval",
        ];

        try {
            // provider null should be rejected
            $metadataApi->postBucketMetadata($this->getTestBucketId(), null, [$md]);
            $this->fail("provider is required");
        } catch (ClientException $e) {
            $this->assertEquals("storage.metadata.invalidProvider", $e->getStringCode());
            $this->assertEquals("Provider is required.", $e->getMessage());
        }

        // invalid characters in provider
        try {
            $metadataApi->postBucketMetadata($this->getTestBucketId(), "%invalidCharacter$", [$md]);
            $this->fail("Invalid metadata provider");
        } catch (ClientException $e) {
            $this->assertEquals("storage.metadata.invalidProvider", $e->getStringCode());
            $this->assertEquals("Invalid metadata provider: %invalidCharacter$", $e->getMessage());
        }
    }

    public function testTryToRemoveForeignData()
    {
        $medataApi = new Metadata($this->_client);
        $md = [
            'key' => 'magic-key',
            'value' => 'magic-frog'
        ];

        $createdMetadata = $medataApi->postBucketMetadata($this->getTestBucketId(), 'provider', [$md]);
        $anotherBucketId = $this->getTestBucketId(self::STAGE_OUT);

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('The supplied metadata ID was not found');
        $medataApi->deleteBucketMetadata($anotherBucketId, $createdMetadata[0]['id']);
    }

    public function testTryToRemoveForeignMetadataFromTable()
    {
        $medataApi = new Metadata($this->_client);
        $md = [
            'key' => 'magic-key',
            'value' => 'magic-frog'
        ];

        $tableId = $this->getTestBucketId() . '.table';
        $createdMetadata = $medataApi->postTableMetadata($tableId, 'provider', [$md]);

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('The supplied metadata ID was not found');
        $medataApi->deleteBucketMetadata($this->getTestBucketId(), $createdMetadata[0]['id']);
    }

    public function testBucketMetadataForTokenWithReadPrivilege()
    {
        $testMetadataValue = 'testval';

        $bucketId = $this->getTestBucketId();
        $metadataApi = new Metadata($this->_client);

        $provider = self::TEST_PROVIDER;
        $metadataApi->postBucketMetadata(
            $bucketId,
            $provider,
            [
                [
                    'key' => self::TEST_METADATA_KEY_1,
                    'value' => $testMetadataValue,
                ]
            ]
        );


        $readClient = $this->getClient([
            'token' => $this->prepareTokenWithReadPrivilegeForBucket($bucketId),
            'url' => STORAGE_API_URL,
            'backoffMaxTries' => 1,
        ]);

        $readMetadataApi = new Metadata($readClient);

        $metadataArray = $readMetadataApi->listBucketMetadata($bucketId);
        $this->assertCount(1, $metadataArray);

        $metadata = reset($metadataArray);
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
                    ]
                ]
            );

            $this->fail('Token with read permissions should not update metadata');
        } catch (ClientException $e) {
            $this->assertEquals(403, $e->getCode());
        }

        $metadataArray = $readMetadataApi->listBucketMetadata($bucketId);
        $this->assertCount(1, $metadataArray);

        $metadata = reset($metadataArray);
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
                    ]
                ]
            );

            $this->fail('Token with read permissions should not create metadata');
        } catch (ClientException $e) {
            $this->assertEquals(403, $e->getCode());
        }

        $metadataArray = $readMetadataApi->listBucketMetadata($bucketId);
        $this->assertCount(1, $metadataArray);
    }

    public function apiEndpoints()
    {
        $tableId = '.table';
        $columnId = $tableId . '.id';
        return [
            'columnEndpoint' => [self::ENDPOINT_TYPE_COLUMNS, $columnId],
            'tableEndpoint' => [self::ENDPOINT_TYPE_TABLES, $tableId],
            'bucketEndpoint' => [self::ENDPOINT_TYPE_BUCKETS, ""],
        ];
    }


    private function postMetadata($apiEndpoint, $objId, $metadata)
    {
        $metadataApi = new Metadata($this->_client);
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
        $metadataApi = new Metadata($this->_client);
        switch ($apiEndpoint) {
            case self::ENDPOINT_TYPE_COLUMNS:
                $res = $metadataApi->deleteColumnMetadata($objId, $metadataId);
                break;
            case self::ENDPOINT_TYPE_TABLES:
                $res = $metadataApi->deleteTableMetadata($objId, $metadataId);
                break;
            case self::ENDPOINT_TYPE_BUCKETS:
                $res = $metadataApi->deleteBucketMetadata($objId, $metadataId);
                break;
        }
    }

    private function prepareTokenWithReadPrivilegeForBucket($bucketId)
    {
        $options = new TokenCreateOptions();
        $options
            ->setExpiresIn(60 * 5)
            ->setDescription(sprintf('Test read of "%s" bucket', $bucketId))
            ->addBucketPermission($bucketId, TokenAbstractOptions::BUCKET_PERMISSION_READ)
        ;

        $token = $this->tokens->createToken($options);
        return $token['token'];
    }
}
