<?php

declare(strict_types=1);

namespace Keboola\Test\Backend\ExternalBuckets;

use Google\Cloud\BigQuery\AnalyticsHub\V1\AnalyticsHubServiceClient;
use Google\Cloud\BigQuery\AnalyticsHub\V1\DataExchange;
use Google\Cloud\BigQuery\AnalyticsHub\V1\Listing;
use Google\Cloud\BigQuery\AnalyticsHub\V1\Listing\BigQueryDatasetSource;
use Google\Cloud\BigQuery\BigQueryClient;
use Google\Cloud\Iam\V1\Binding;
use GuzzleHttp\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Workspaces;
use Keboola\Test\Backend\Workspaces\Backend\BigQueryClientHandler;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;
use Keboola\Test\Utils\EventsQueryBuilder;

class BigqueryRegisterBucketTest extends BaseExternalBuckets
{
    public function setUp(): void
    {
        parent::setUp();
    }

    public function testRegisterBucket(): void
    {
        $this->initEvents($this->_client);
        $token = $this->_client->verifyToken();

        if (!in_array('input-mapping-read-only-storage', $token['owner']['features'])) {
            $this->markTestSkipped(sprintf('Read only mapping is not enabled for project "%s"', $token['owner']['id']));
        }
        if (!in_array('external-buckets', $token['owner']['features'])) {
            $this->markTestSkipped(sprintf('External buckets are not enabled for project "%s"', $token['owner']['id']));
        }
        $this->allowTestForBackendsOnly([self::BACKEND_BIGQUERY], 'Backend has to support external buckets');
        $this->expectNotToPerformAssertions();
    }

    public function testInvalidDBToRegister(): void
    {
        $this->dropBucketIfExists($this->_client, 'in.test-bucket-registration', true);

        try {
            $this->_client->registerBucket(
                'test-bucket-registration',
                ['non-existing-database', 'non-existing-schema'],
                'in',
                'will fail',
                'bigquery',
                'test-bucket-will-fail'
            );
            $this->fail('should fail');
        } catch (ClientException $e) {
            $this->assertSame('storage.dbObjectNotFound', $e->getStringCode());
            $this->assertStringContainsString(
                'Could not map bindings for google.cloud.bigquery.analyticshub.v1.AnalyticsHubService/SubscribeListing to any Uri template.',
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

        // prepare workspace
        $createdListing = $this->prepareExternalBucketForRegistration($description);

        // register workspace as external bucket
        $runId = $this->setRunId();
        $idOfBucket = $this->_client->registerBucket(
            $testBucketName,
            [$createdListing->getName()],
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
        $externalCredentials['connection']['schema'] = sha1($description). '_external_bucket';

        // add first table to workspace
        $db = WorkspaceBackendFactory::createWorkspaceBackend($externalCredentials);
        $db->createTable('TEST', [
            'AMOUNT' => 'INT',
            'DESCRIPTION' => 'STRING',
        ]);
        $db->executeQuery(sprintf(
        /** @lang BigQuery */
            'INSERT INTO `%s`.`TEST` (`AMOUNT`, `DESCRIPTION`) VALUES (1, \'test\');',
            $externalCredentials['connection']['schema']
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
            'INT64',
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

        // add second table to workspace
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

        // alter first table, drop second table, add third table to workspace
        $db->dropTable('TEST2');
        $db->executeQuery(sprintf(
        /** @lang BigQuery */
            'ALTER TABLE `%s`.`TEST` DROP COLUMN `AMOUNT`',
            $externalCredentials['connection']['schema']
        ));
        $db->executeQuery(sprintf(
        /** @lang BigQuery */
            'ALTER TABLE `%s`.`TEST` ADD COLUMN `XXX` FLOAT64',
            $externalCredentials['connection']['schema']
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

//        todo view is not supported in driver
//        $db->executeQuery(sprintf(
//        /** @lang BigQuery */
//            'CREATE VIEW `%s`.`MY_VIEW` AS SELECT * FROM `%s`.`TEST`',
//            $externalCredentials['connection']['schema'],
//            $externalCredentials['connection']['schema'],
//        ));
//
//        $runId = $this->setRunId();
//        $this->_client->refreshBucket($idOfBucket);
//
//        $assertCallback = function ($events) {
//            $this->assertCount(1, $events);
//        };
//        $query = new EventsQueryBuilder();
//        $query->setEvent('storage.tableCreated')
//            ->setTokenId($this->tokenId)
//            ->setRunId($runId);
//        $this->assertEventWithRetries($this->_client, $assertCallback, $query);
//
//        $assertCallback = function ($events) {
//            $this->assertCount(1, $events);
//        };
//        $query = new EventsQueryBuilder();
//        $query->setEvent('storage.bucketRefreshed')
//            ->setTokenId($this->tokenId)
//            ->setRunId($runId);
//        $this->assertEventWithRetries($this->_client, $assertCallback, $query);
//
//        // check external bucket
//        $tables = $this->_client->listTables($idOfBucket);
//        $this->assertCount(3, $tables);

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
                            'source' => $tables[0]['id'],
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

    private function prepareExternalBucketForRegistration(string $description): Listing
    {
        $bucketSchemaName = sha1($description). '_external_bucket';
        $externalCredentials = $this->getCredentialsArray();
        $externalProjectStringId = $externalCredentials['project_id'];

        $dataExchangeId = sha1($description) . str_replace('-', '_', $externalProjectStringId);
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

        return $createdListing;
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
}
