<?php

namespace Keboola\Test\Backend\CommonPart1;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\Test\Backend\TableWithConfigurationUtils;
use Keboola\Test\ClientProvider\ClientProvider;
use Keboola\Test\StorageApiTestCase;
use Keboola\Test\Utils\EventTesterUtils;

class TableWithConfigurationLoadTest extends StorageApiTestCase
{
    use EventTesterUtils;
    use TableWithConfigurationUtils;

    private Client $client;

    private ClientProvider $clientProvider;

    private Components $componentsClient;

    public function setUp(): void
    {
        parent::setUp();

        // check feature
        $token = $this->_client->verifyToken();
        if (!in_array('tables-with-configuration', $token['owner']['features'])) {
            $this->markTestSkipped(sprintf('Creating tables from configurations feature is not enabled for project "%s"', $token['owner']['id']));
        }

        if ($token['owner']['defaultBackend'] !== self::BACKEND_SYNAPSE) {
            self::markTestSkipped(sprintf(
                'Backend "%s" is not supported tables with configuration',
                $token['owner']['defaultBackend']
            ));
        }

        // init buckets
        $this->initEmptyTestBucketsForParallelTests();

        $this->clientProvider = new ClientProvider($this);
        $this->client = $this->clientProvider->createClientForCurrentTest();

        $this->assertComponentExists();

        $this->configId = sha1($this->generateDescriptionForTestObject());

        $this->dropTableAndConfiguration($this->configId);

        $this->initEvents($this->client);
    }

    public function testTableLoadFromFile(): void
    {
        $tableName = 'custom-table-1';

        // HTML NOWDOC used so that autoformat does not reformat SQL queries inside the strings
        $tableId = $this->prepareTableWithConfiguration($tableName, [
                'migrations' => [
                    [

                        'sql' => <<<'HTML'
CREATE TABLE {{ id(bucketName) }}.{{ id(tableName) }} ([id] INTEGER, [NAME] VARCHAR(100))
HTML,
                        'description' => 'first ever',
                    ],
                ],
                'ingestionFullLoad' => [
                    'queries' => [
                        [
                            'sql' => <<<'HTML'
CREATE TABLE {{ id(stageSchemaName) }}.{{ id(stageTableName) }} ([id] INTEGER, [NAME] VARCHAR(100)) WITH (DISTRIBUTION = ROUND_ROBIN,CLUSTERED COLUMNSTORE INDEX)
HTML
                            ,
                            'description' => '',
                        ],
                        [
                            'sql' => <<<'HTML'
CREATE TABLE {{ id(destSchemaName) }}.{{ id(destTableName) }} ([id] INTEGER, [NAME] VARCHAR(100), [_timestamp] DATETIME2) WITH (DISTRIBUTION = ROUND_ROBIN,CLUSTERED COLUMNSTORE INDEX)
HTML
                            ,
                            'description' => '',
                        ],
                        [
                            'sql' => <<<'HTML'
COPY INTO {{ id(stageSchemaName) }}.{{ id(stageTableName) }}
FROM {{ sourceFile1 }}
WITH (
    FILE_TYPE='CSV',
    CREDENTIAL=(IDENTITY='Shared Access Signature', SECRET='?sourceSasToken634fc11aae8fd349870618'),
    FIELDQUOTE='"',
    FIELDTERMINATOR=',',
    ENCODING = 'UTF8',
    
    IDENTITY_INSERT = 'OFF'
    ,FIRSTROW=2
)
HTML
                            ,
                            'description' => '',
                        ],
                        [
                            'sql' => <<<'HTML'
CREATE TABLE {{ id(destSchemaName) }}.{{ id(destTableName ~ '_tmp') }} WITH (DISTRIBUTION=ROUND_ROBIN,CLUSTERED COLUMNSTORE INDEX) AS SELECT [id],[NAME] FROM {{ id(stageSchemaName) }}.{{ id(stageTableName) }}
HTML
                            ,
                            'description' => '',
                        ],
                        [
                            'sql' => <<<'HTML'
RENAME OBJECT {{ id(destSchemaName) }}.{{ id(destTableName) }} TO {{ id(destSchemaName ~ '_rename')}}
HTML
                            ,
                            'description' => '',
                        ],
                        [
                            'sql' => <<<'HTML'
RENAME OBJECT {{ id(destSchemaName) }}.{{ id(destTableName ~ '_tmp') }} TO {{ id(destTableName) }}
HTML
                            ,
                            'description' => '',
                        ],
                        [
                            'sql' => <<<'HTML'
DROP TABLE {{ id(destSchemaName) }}.{{ id(destSchemaName ~ '_rename')}}
HTML
                            ,
                            'description' => '',
                        ],
                        [
                            'sql' => <<<'HTML'
IF OBJECT_ID (N'{{ id(destSchemaName) }}.{{ id(destTableName ~ '_tmp') }}', N'U') IS NOT NULL DROP TABLE {{ id(destSchemaName) }}.{{ id(destTableName ~ '_tmp') }}
HTML
                            ,
                            'description' => '',
                        ],
                        [
                            'sql' => <<<'HTML'
IF OBJECT_ID (N'{{ id(destSchemaName) }}.{{ id(destSchemaName ~ '_rename')}}', N'U') IS NOT NULL DROP TABLE {{ id(destSchemaName) }}.{{ id(destSchemaName ~ '_rename')}}
HTML
                            ,

                        ],
                    ],
                ],
            ]
        );

        $csvFile = new CsvFile(__DIR__ . '/../../_data/languages-without-headers.csv');
        $fileId = $this->_client->uploadFile(
            $csvFile->getPathname(),
            (new FileUploadOptions())
                ->setNotify(false)
                ->setIsPublic(false)
                ->setCompress(true)
                ->setTags(['table-import'])
        );

        $this->_client->writeTableAsyncDirect($tableId, [
            'dataFileId' => $fileId,
        ]);

        $table = $this->_client->getTable($tableId);

        $this->assertEquals(['id', 'name_another'], $table['columns']);
        $this->assertSame(5, $table['rowsCount']);
        $this->assertTableColumnMetadata([
            'id' => [
                'KBC.datatype.type' => 'INT',
                'KBC.datatype.nullable' => '1',
                'KBC.datatype.basetype' => 'INTEGER',
            ],
            'name_another' => [
                'KBC.datatype.type' => 'VARCHAR',
                'KBC.datatype.nullable' => '1',
                'KBC.datatype.basetype' => 'STRING',
                'KBC.datatype.length' => '50',
            ],
        ], $table);

        // check events
        $events = $this->listEventsFilteredByName($this->client, 'storage.tableLoadWithQueries', $tableId, 50);
        $this->assertCount(9, $events);

        $events = $this->listEventsFilteredByName($this->client, 'storage.tableImportDone', $tableId, 10);
        $this->assertCount(1, $events);
    }
}
