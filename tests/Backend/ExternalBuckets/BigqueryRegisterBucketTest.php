<?php

declare(strict_types=1);

namespace Keboola\Test\Backend\ExternalBuckets;

use Generator;
use Google\Cloud\BigQuery\AnalyticsHub\V1\AnalyticsHubServiceClient;
use Google\Cloud\BigQuery\AnalyticsHub\V1\DataExchange;
use Google\Cloud\BigQuery\AnalyticsHub\V1\Listing;
use Google\Cloud\BigQuery\AnalyticsHub\V1\Listing\BigQueryDatasetSource;
use Google\Cloud\BigQuery\BigQueryClient;
use Google\Cloud\Iam\V1\Binding;
use Google\Cloud\Storage\StorageClient;
use GuzzleHttp\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Workspaces;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use Keboola\Test\Backend\Workspaces\Backend\BigQueryClientHandler;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;
use Keboola\Test\Utils\EventsQueryBuilder;

// Tests for registering an external bucket that contains different types of tables.
// Available table types https://cloud.google.com/bigquery/docs/information-schema-tables#schema:
// - BASE TABLE: standard table
// - CLONE: we don't test this, it's just an aperture
// - SNAPSHOT
// - VIEW
// - MATERIALIZED VIEW
// - EXTERNAL
class BigqueryRegisterBucketTest extends BaseExternalBuckets
{
    public function setUp(): void
    {
        parent::setUp();

        $this->initEvents($this->_client);
        $token = $this->_client->verifyToken();

        if (!in_array('external-buckets', $token['owner']['features'])) {
            $this->markTestSkipped(sprintf('External buckets are not enabled for project "%s"', $token['owner']['id']));
        }
        $this->allowTestForBackendsOnly([self::BACKEND_BIGQUERY], 'Backend has to support external buckets');
    }

    public function testInvalidListingToRegister(): void
    {
        $this->dropBucketIfExists($this->_client, 'in.test-bucket-registration', true);

        try {
            $this->_client->registerBucket(
                'test-bucket-registration',
                ['path'],
                'in',
                'will fail',
                'bigquery',
                'test-bucket-will-fail'
            );
            $this->fail('should fail');
        } catch (ClientException $e) {
            $this->assertSame('storage.buckets.validation', $e->getStringCode());
            $this->assertStringContainsString(
                'Invalid path for Bigquery backend. Path must have exactly four elements, project id, location, exchanger id, listing id',
                $e->getMessage()
            );
        }
    }

    public function testNotExistListingToRegister(): void
    {
        $this->dropBucketIfExists($this->_client, 'in.test-bucket-registration', true);

        try {
            $this->_client->registerBucket(
                'test-bucket-registration',
                ['132', 'us', 'non_exist', 'non_exist'],
                'in',
                'will fail',
                'bigquery',
                'test-bucket-will-fail'
            );
            $this->fail('should fail');
        } catch (ClientException $e) {
            $this->assertSame('storage.dbObjectNotFound', $e->getStringCode());
            $this->assertStringContainsString(
                'Failed to register external bucket "test-bucket-registration" permission denied for subscribe listing "projects/132/locations/us/dataExchanges/non_exist/listings/non_exist"',
                $e->getMessage()
            );
        }
    }

    public function testRegisterWSAsExternalBucket(): void
    {
        $description = $this->generateDescriptionForTestObject();
        $testBucketName = $this->getTestBucketName($description);
        $bucketId = self::STAGE_IN . '.' . $testBucketName;

        $this->dropBucketIfExists($this->_client, $bucketId, true);
        $this->initEvents($this->_client);

        $externalBucketBackend = 'bigquery';
        $guide = $this->_client->registerBucketGuide(['external_bucket'], $externalBucketBackend);
        $this->assertArrayHasKey('markdown', $guide);
        $this->assertStringContainsString('## Create a New Exchange', $guide['markdown']);
        $this->assertStringContainsString('## Create a New Listing', $guide['markdown']);

        // prepare external bucket
        $path = $this->prepareExternalBucketForRegistration($description);

        // register external bucket
        $runId = $this->setRunId();
        $idOfBucket = $this->_client->registerBucket(
            $testBucketName,
            $path,
            'in',
            'Iam in external bucket',
            $externalBucketBackend,
            'Iam-your-external-bucket'
        );

        $assertCallback = function ($events) {
            $this->assertCount(1, $events);
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.bucketCreated')
            ->setTokenId($this->tokenId)
            ->setRunId($runId);
        $this->assertEventWithRetries($this->_client, $assertCallback, $query);

        // check external bucket
        $bucket = $this->_client->getBucket($idOfBucket);
        $this->assertTrue($bucket['hasExternalSchema']);

        $tables = $this->_client->listTables($idOfBucket);
        $this->assertCount(0, $tables);
        // check external bucket

        $externalCredentials['connection']['backend'] = 'bigquery';
        $externalCredentials['connection']['credentials'] = $this->getCredentialsArray();
        $externalCredentials['connection']['schema'] = sha1($description) . '_external_bucket';

        // add first table to external bucket
        // I created a user for the external bucket the same way as for WS.
        // Workspace can't just be used because the user doesn't have the right to create the exchanger and the listing
        $db = WorkspaceBackendFactory::createWorkspaceBackend($externalCredentials);
        $db->createTable('TEST', [
            'AMOUNT' => 'INT',
            'DESCRIPTION' => 'STRING',
        ]);
        $db->executeQuery(sprintf(
        /** @lang BigQuery */
            'INSERT INTO %s.`TEST` (`AMOUNT`, `DESCRIPTION`) VALUES (1, \'test\');',
            BigqueryQuote::quoteSingleIdentifier($externalCredentials['connection']['schema'])
        ));

        // refresh external bucket
        $runId = $this->setRunId();
        $this->_client->refreshBucket($idOfBucket);

        $assertCallback = function ($events) {
            $this->assertCount(1, $events);
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.tableCreated')
            ->setTokenId($this->tokenId)
            ->setRunId($runId);
        $this->assertEventWithRetries($this->_client, $assertCallback, $query);

        $assertCallback = function ($events) {
            $this->assertCount(1, $events);
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.bucketRefreshed')
            ->setTokenId($this->tokenId)
            ->setRunId($runId);
        $this->assertEventWithRetries($this->_client, $assertCallback, $query);

        // check external bucket
        $tables = $this->_client->listTables($idOfBucket);
        $this->assertCount(1, $tables);
        $tableDetail = $this->_client->getTable($tables[0]['id']);

        $this->assertSame('KBC.dataTypesEnabled', $tableDetail['metadata'][0]['key']);
        $this->assertSame('true', $tableDetail['metadata'][0]['value']);
        $this->assertTrue($tableDetail['isTyped']);

        $this->assertCount(2, $tableDetail['columns']);

        $this->assertColumnMetadata(
            'INTEGER',
            '1',
            'INTEGER',
            null,
            $tableDetail['columnMetadata']['AMOUNT']
        );
        $this->assertColumnMetadata(
            'STRING',
            '1',
            'STRING',
            null,
            $tableDetail['columnMetadata']['DESCRIPTION']
        );

        // export table from external bucket
        $this->_client->exportTableAsync($tables[0]['id']);

        $preview = $this->_client->getTableDataPreview($tables[0]['id']);
        // expect two lines in preview because of the header
        $this->assertCount(2, \Keboola\StorageApi\Client::parseCsv($preview, false));

        // add second table to external bucket
        $db->createTable('TEST2', ['AMOUNT' => 'INT', 'DESCRIPTION' => 'STRING']);

        // refresh external bucket
        $runId = $this->setRunId();
        $this->_client->refreshBucket($idOfBucket);

        $assertCallback = function ($events) {
            $this->assertCount(1, $events);
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.tableCreated')
            ->setTokenId($this->tokenId)
            ->setRunId($runId);
        $this->assertEventWithRetries($this->_client, $assertCallback, $query);

        $assertCallback = function ($events) {
            $this->assertCount(1, $events);
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.bucketRefreshed')
            ->setTokenId($this->tokenId)
            ->setRunId($runId);
        $this->assertEventWithRetries($this->_client, $assertCallback, $query);

        // check external bucket
        $tables = $this->_client->listTables($idOfBucket);
        $this->assertCount(2, $tables);

        // alter first table, drop second table, add third table to external bucket
        $db->dropTable('TEST2');
        $db->executeQuery(sprintf(
        /** @lang BigQuery */
            'ALTER TABLE %s.`TEST` DROP COLUMN `AMOUNT`',
            BigqueryQuote::quoteSingleIdentifier($externalCredentials['connection']['schema'])
        ));
        $db->executeQuery(sprintf(
        /** @lang BigQuery */
            'ALTER TABLE %s.`TEST` ADD COLUMN `XXX` FLOAT64',
            BigqueryQuote::quoteSingleIdentifier($externalCredentials['connection']['schema'])
        ));
        $db->createTable('TEST3', ['AMOUNT' => 'INT', 'DESCRIPTION' => 'STRING']);

        // refresh external bucket
        $runId = $this->setRunId();
        $this->_client->refreshBucket($idOfBucket);

        $assertCallback = function ($events) {
            $this->assertCount(1, $events);
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.tableDeleted')
            ->setTokenId($this->tokenId)
            ->setRunId($runId);
        $this->assertEventWithRetries($this->_client, $assertCallback, $query);

        $assertCallback = function ($events) {
            $this->assertCount(1, $events);
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.tableCreated')
            ->setTokenId($this->tokenId)
            ->setRunId($runId);
        $this->assertEventWithRetries($this->_client, $assertCallback, $query);

        $assertCallback = function ($events) {
            $this->assertCount(1, $events);
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.tableColumnsUpdated')
            ->setTokenId($this->tokenId)
            ->setRunId($runId);
        $this->assertEventWithRetries($this->_client, $assertCallback, $query);

        $assertCallback = function ($events) {
            $this->assertCount(1, $events);
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.bucketRefreshed')
            ->setTokenId($this->tokenId)
            ->setRunId($runId);
        $this->assertEventWithRetries($this->_client, $assertCallback, $query);

        // check external bucket
        $tables = $this->_client->listTables($idOfBucket);
        $this->assertCount(2, $tables);

        $tableDetail = $this->_client->getTable($tables[0]['id']);
        $this->assertSame(['DESCRIPTION', 'XXX'], $tableDetail['columns']);

        $this->assertColumnMetadata(
            'STRING',
            '1',
            'STRING',
            null,
            $tableDetail['columnMetadata']['DESCRIPTION']
        );

        $this->assertColumnMetadata(
            'FLOAT64',
            '1',
            'FLOAT',
            null,
            $tableDetail['columnMetadata']['XXX']
        );

        $db->executeQuery(sprintf(
        /** @lang BigQuery */
            'CREATE VIEW `%s`.`MY_VIEW` AS SELECT * FROM `%s`.`TEST`',
            $externalCredentials['connection']['schema'],
            $externalCredentials['connection']['schema'],
        ));

        $runId = $this->setRunId();
        $this->_client->refreshBucket($idOfBucket);

        $assertCallback = function ($events) {
            $this->assertCount(1, $events);
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.tableCreated')
            ->setTokenId($this->tokenId)
            ->setRunId($runId);
        $this->assertEventWithRetries($this->_client, $assertCallback, $query);

        $assertCallback = function ($events) {
            $this->assertCount(1, $events);
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.bucketRefreshed')
            ->setTokenId($this->tokenId)
            ->setRunId($runId);
        $this->assertEventWithRetries($this->_client, $assertCallback, $query);

        // check external bucket
        $tables = $this->_client->listTables($idOfBucket);
        $this->assertCount(3, $tables);

        $ws = new Workspaces($this->_client);
        $workspace = $ws->createWorkspace();

        // try failing load
        try {
            $ws->cloneIntoWorkspace(
                $workspace['id'],
                [
                    'input' => [
                        [
                            'source' => $tables[0]['id'],
                            'destination' => 'test',
                        ],
                    ],
                ]
            );
            $this->fail('Should fail');
        } catch (ClientException $e) {
            $this->assertSame('APPLICATION_ERROR', $e->getStringCode());
            $this->assertStringContainsString(
                'Cloning data into workspaces is only supported for Snowflake,',
                $e->getMessage()
            );
        }

        try {
            $ws->loadWorkspaceData(
                $workspace['id'],
                [
                    'input' => [
                        [
                            'source' => $tables[1]['id'],
                            'destination' => 'test',
                        ],
                    ],
                ]
            );
            $this->fail('Should fail');
        } catch (ClientException $e) {
            $this->assertSame('workspace.tableCannotBeLoaded', $e->getStringCode());
            $this->assertStringContainsString(
                sprintf(
                    'Table "%s" is part of external bucket "%s.TEST" and cannot be loaded into workspace.', // todo fix err msg in connection
                    $testBucketName,
                    $bucketId
                ),
                $e->getMessage()
            );
        }
    }

    public function testRegistrationOfExternalTableFromCsv(): void
    {
        $description = $this->generateDescriptionForTestObject();
        $testBucketName = $this->getTestBucketName($description);
        $bucketId = self::STAGE_IN . '.' . $testBucketName;

        $this->dropBucketIfExists($this->_client, $bucketId, true);
        $this->initEvents($this->_client);

        // prepare external bucket
        $path = $this->prepareExternalBucketForRegistration($description);

        // register external bucket
        $idOfBucket = $this->_client->registerBucket(
            $testBucketName,
            $path,
            'in',
            'Iam in external table from csv',
            'bigquery',
            'Iam-your-external-bucket-for-external-table'
        );

        // check external bucket
        $bucket = $this->_client->getBucket($idOfBucket);
        $this->assertTrue($bucket['hasExternalSchema']);

        $tables = $this->_client->listTables($idOfBucket);
        $this->assertCount(0, $tables);

        $this->createExternalTable($description);

        // refresh external bucket
        $this->_client->refreshBucket($idOfBucket);

        // check external bucket
        $tables = $this->_client->listTables($idOfBucket);
        $this->assertCount(1, $tables);

        $this->_client->exportTableAsync($tables[0]['id']);

        $preview = $this->_client->getTableDataPreview($tables[0]['id']);
        $this->assertCount(6, \Keboola\StorageApi\Client::parseCsv($preview, false));
    }

    /**
     * @dataProvider createOtherObjectsProvider
     */
    public function testRegistrationOtherObjects(string $objectName, string $query): void
    {
        $description = $this->generateDescriptionForTestObject();
        $testBucketName = $this->getTestBucketName($description);
        $bucketId = self::STAGE_IN . '.' . $testBucketName;

        $this->dropBucketIfExists($this->_client, $bucketId, true);
        $this->initEvents($this->_client);

        // prepare external bucket
        $path = $this->prepareExternalBucketForRegistration($description);

        // register external bucket
        $idOfBucket = $this->_client->registerBucket(
            $testBucketName,
            $path,
            'in',
            'Iam in external bucket',
            'bigquery',
            'Iam-your-external-bucket-' . $objectName
        );

        $tables = $this->_client->listTables($idOfBucket);
        $this->assertCount(0, $tables);
        // check external bucket

        $externalCredentials['connection']['backend'] = 'bigquery';
        $externalCredentials['connection']['credentials'] = $this->getCredentialsArray();
        $externalCredentials['connection']['schema'] = sha1($description) . '_external_bucket';

        $db = WorkspaceBackendFactory::createWorkspaceBackend($externalCredentials);
        $db->createTable('TEST', [
            'AMOUNT' => 'INT',
            'DESCRIPTION' => 'STRING',
        ]);
        $db->executeQuery(sprintf(
        /** @lang BigQuery */
            'INSERT INTO %s.`TEST` (`AMOUNT`, `DESCRIPTION`) VALUES (1, \'test\');',
            BigqueryQuote::quoteSingleIdentifier($externalCredentials['connection']['schema'])
        ));

        // refresh external bucket
        $this->_client->refreshBucket($idOfBucket);

        $tables = $this->_client->listTables($idOfBucket);
        $this->assertCount(1, $tables);

        // create object in external bucket, by query from provider
        $db->executeQuery(
            sprintf(
                $query,
                BigqueryQuote::quoteSingleIdentifier($externalCredentials['connection']['schema']),
                BigqueryQuote::quoteSingleIdentifier($externalCredentials['connection']['schema'])
            )
        );

        // refresh external bucket
        $this->_client->refreshBucket($idOfBucket);

        $tables = $this->_client->listTables($idOfBucket);

        $this->assertCount(2, $tables);

        // test if exist object created by query from provider
        $table = $this->_client->getTable($idOfBucket.'.'.$objectName);
        $this->_client->exportTableAsync($table['id']);

        $preview = $this->_client->getTableDataPreview($table['id']);
        $this->assertCount(2, \Keboola\StorageApi\Client::parseCsv($preview, false));
    }

    public function createOtherObjectsProvider(): Generator
    {
        yield 'create materialized view' => [
            'my_view',
            'CREATE MATERIALIZED VIEW %s.`my_view` AS SELECT * FROM %s.`TEST`',
        ];

        yield 'create snapshot' => [
            'snapshot',
            'CREATE SNAPSHOT TABLE %s.`snapshot` CLONE %s.`TEST` OPTIONS ( expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR));',
        ];
    }

    /**
     * @return string[]
     */
    private function prepareExternalBucketForRegistration(string $description): array
    {
        $bucketSchemaName = sha1($description) . '_external_bucket';
        $externalCredentials = $this->getCredentialsArray();
        $externalProjectStringId = $externalCredentials['project_id'];

        // get last 63 chars becauase the displayName has limit
        $dataExchangeId = substr(
            sha1($description) . str_replace('-', '_', $externalProjectStringId),
            -63
        );
        $location = 'US';
        $analyticHubClient = $this->getAnalyticsHubServiceClient($externalCredentials);

        $formattedParent = $analyticHubClient->locationName($externalProjectStringId, $location);
        $exchangers = $analyticHubClient->listDataExchanges($formattedParent);

        // Delete all exchangers with same prefix
        /** @var DataExchange $exchanger */
        foreach ($exchangers->getIterator() as $exchanger) {
            if (strpos($exchanger->getName(), $dataExchangeId) !== 0) {
                $analyticHubClient->deleteDataExchange($exchanger->getName());
            }
        }

        $bqClient = $this->getBigQueryClient($externalCredentials);

        try {
            $bqClient->dataset($bucketSchemaName)->delete(['deleteContents' => true]);
        } catch (\Exception $e) {
            // ignore if not exist
        }

        $bqClient->createDataset($bucketSchemaName);

        // 1. Create exchanger in source project
        $dataExchange = new DataExchange();
        $dataExchange->setDisplayName($dataExchangeId);
        $dataExchange = $analyticHubClient->createDataExchange(
            $formattedParent,
            $dataExchangeId,
            $dataExchange
        );

        $listingId = str_replace('-', '_', $externalCredentials['project_id']) . '_listing';
        $lst = new BigQueryDatasetSource([
            'dataset' => sprintf(
                'projects/%s/datasets/%s',
                $externalProjectStringId,
                $bucketSchemaName
            ),
        ]);
        $listing = new Listing();
        $listing->setBigqueryDataset($lst);
        $listing->setDisplayName($listingId);

        // 2 Create listing for extern bucket
        $createdListing = $analyticHubClient->createListing($dataExchange->getName(), $listingId, $listing);

        $iamExchangerPolicy = $analyticHubClient->getIamPolicy($dataExchange->getName());
        $binding = $iamExchangerPolicy->getBindings();
        // 3. Add permission to destination project
        $binding[] = new Binding([
            'role' => 'roles/analyticshub.subscriber',
            'members' => ['serviceAccount:' . BQ_DESTINATION_PROJECT_SERVICE_ACC_EMAIL],
        ]);
        $iamExchangerPolicy->setBindings($binding);
        $analyticHubClient->setIamPolicy($dataExchange->getName(), $iamExchangerPolicy);

        $parsedName = AnalyticsHubServiceClient::parseName($createdListing->getName());
        return [
            $parsedName['project'],
            $parsedName['location'],
            $parsedName['data_exchange'],
            $parsedName['listing'],
        ];
    }

    /**
     * @return array{
     * type: string,
     * project_id: string,
     * private_key_id: string,
     * private_key: string,
     * client_email: string,
     * client_id: string,
     * auth_uri: string,
     * token_uri: string,
     * auth_provider_x509_cert_url: string,
     * client_x509_cert_url: string,
     * }
     */
    public static function getCredentialsArray(): array
    {
        /**
         * @var array{
         * type: string,
         * project_id: string,
         * private_key_id: string,
         * private_key: string,
         * client_email: string,
         * client_id: string,
         * auth_uri: string,
         * token_uri: string,
         * auth_provider_x509_cert_url: string,
         * client_x509_cert_url: string,
         * } $credentialsArr
         */
        $credentialsArr = (array) json_decode(BQ_KEY_FILE_FOR_EXTERNAL_BUCKET, true, 512, JSON_THROW_ON_ERROR);

        return $credentialsArr;
    }

    /**
     * @param array $externalCredentials
     * @return AnalyticsHubServiceClient
     * @throws \Google\ApiCore\ValidationException
     */
    public function getAnalyticsHubServiceClient(array $externalCredentials): AnalyticsHubServiceClient
    {
        $analyticHubClient = new AnalyticsHubServiceClient([
            'credentials' => $externalCredentials,
        ]);
        return $analyticHubClient;
    }

    /**
     * @param array $externalCredentials
     * @return BigQueryClient
     */
    public function getBigQueryClient(array $externalCredentials): BigQueryClient
    {
        $handler = new BigQueryClientHandler(new Client());

        $bqClient = new BigQueryClient([
            'keyFile' => $externalCredentials,
            'httpHandler' => $handler,
        ]);
        return $bqClient;
    }

    private function createExternalTable(string $description): void
    {
        $gcsClient = new StorageClient([
            'keyFile' => $this->getCredentialsArray(),
        ]);

        $filePath = __DIR__ . '/../../_data/languages.csv';
        $gcsBucketName = 'my-external-table-bucket3';
        $retBucket = $gcsClient->bucket($gcsBucketName);
        if ($retBucket->exists() === false) {
            $retBucket = $gcsClient->createBucket($gcsBucketName);
        }

        $file = fopen($filePath, 'rb');
        if (!$file) {
            throw new ClientException("Cannot open file {$file}");
        }
        $object = $retBucket->upload(
            $file,
            [
                'name' => 'languages.csv',
            ]
        );

        // this must be done in a real situation by a user who registers an external bucket
        $object->acl()->add('user-'.BQ_DESTINATION_PROJECT_SERVICE_ACC_EMAIL, 'READER');

        $externalCredentials['connection']['backend'] = 'bigquery';
        $externalCredentials['connection']['credentials'] = $this->getCredentialsArray();
        $externalCredentials['connection']['schema'] = sha1($description) . '_external_bucket';

        $db = WorkspaceBackendFactory::createWorkspaceBackend($externalCredentials);
        $db->executeQuery(sprintf(
            "CREATE OR REPLACE EXTERNAL TABLE %s.externalTable OPTIONS (format = 'CSV',uris = [%s]);",
            BigqueryQuote::quoteSingleIdentifier($externalCredentials['connection']['schema']),
            BigqueryQuote::quote($object->gcsUri())
        ));
    }
}
