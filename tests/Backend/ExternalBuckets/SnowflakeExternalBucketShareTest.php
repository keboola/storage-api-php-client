<?php

declare(strict_types=1);

namespace Keboola\Test\Backend\ExternalBuckets;

use Exception;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Workspaces;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Keboola\Test\Backend\WorkspaceConnectionTrait;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;
use Keboola\Test\Utils\ConnectionUtils;
use const STORAGE_API_LINKING_TOKEN;

class SnowflakeExternalBucketShareTest extends BaseExternalBuckets
{
    use WorkspaceConnectionTrait;
    use ConnectionUtils;

    public const EXTERNAL_DB = 'EXT_DB';
    public const EXTERNAL_SCHEMA = 'EXT_SCHEMA';
    public const EXTERNAL_TABLE = 'EXT_TABLE';

    /** @var Client */
    protected $shareClient;

    /**
     * @var Client
     */
    protected $linkingClient;

    public function setUp(): void
    {
        parent::setUp();
        $this->initEmptyTestBucketsForParallelTests();
        $this->shareClient = $this->getClientForToken(
            STORAGE_API_SHARE_TOKEN,
        );
        $this->linkingClient = $this->getClientForToken(
            STORAGE_API_LINKING_TOKEN,
        );
    }

    public function testShareExternalBucket(): void
    {
        $stage = 'in';
        $bucketName = 'test-bucket-ext-share';

        $this->dropBucketIfExists($this->_client, $stage.'.'.$bucketName, true);

        $guide = $this->_client->registerBucketGuide([self::EXTERNAL_DB, self::EXTERNAL_SCHEMA], 'snowflake');

        $guideExploded = explode("\n", $guide['markdown']);
        $db = $this->ensureSnowflakeConnection();

        $db->executeQuery(
            sprintf(
                'DROP DATABASE IF EXISTS %s;',
                SnowflakeQuote::quoteSingleIdentifier(self::EXTERNAL_DB),
            ),
        );
        $db->executeQuery(
            sprintf(
                'CREATE DATABASE %s;',
                SnowflakeQuote::quoteSingleIdentifier(self::EXTERNAL_DB),
            ),
        );
        $db->executeQuery(
            sprintf(
                'USE DATABASE %s;',
                SnowflakeQuote::quoteSingleIdentifier(self::EXTERNAL_DB),
            ),
        );
        $db->executeQuery(
            sprintf(
                'CREATE SCHEMA %s;',
                SnowflakeQuote::quoteSingleIdentifier(self::EXTERNAL_SCHEMA),
            ),
        );
        $db->executeQuery(
            sprintf(
                'USE SCHEMA %s;',
                SnowflakeQuote::quoteSingleIdentifier(self::EXTERNAL_SCHEMA),
            ),
        );
        $db->executeQuery(
            sprintf(
                'CREATE TABLE %s (ID INT, LASTNAME VARCHAR(255));',
                SnowflakeQuote::quoteSingleIdentifier(self::EXTERNAL_TABLE),
            ),
        );

        foreach ($guideExploded as $command) {
            if (str_starts_with($command, 'GRANT') && !str_contains($command, 'FUTURE')) {
                try {
                    $db->executeQuery($command);
                } catch (Exception $e) {
                    $this->fail($e->getMessage() . ': ' . $command);
                }
            }
        }

        $registeredBucketId = $this->_client->registerBucket(
            $bucketName,
            [self::EXTERNAL_DB, self::EXTERNAL_SCHEMA],
            $stage,
            'will not fail',
            'snowflake',
            $bucketName,
        );

        $tables = $this->_client->listTables($stage.'.'.$bucketName);
        $this->assertCount(1, $tables);

        $shareToken = $this->linkingClient->verifyToken();
        $targetProjectId = $shareToken['owner']['id'];

        $this->shareClient->shareBucketToProjects($registeredBucketId, [$targetProjectId]);

        $sharedBucket = $this->_client->getBucket($registeredBucketId);
        $this->assertEquals('specific-projects', $sharedBucket['sharing']);
        $this->assertEquals($targetProjectId, $sharedBucket['sharingParameters']['projects'][0]['id']);

        $this->shareClient->unshareBucket($registeredBucketId);
        $unsharedBucket = $this->_client->getBucket($registeredBucketId);
        $this->assertNull($unsharedBucket['sharing']);

        $db->executeQuery(
            sprintf(
                'DROP DATABASE %s;',
                SnowflakeQuote::quoteSingleIdentifier(self::EXTERNAL_DB),
            ),
        );
    }

    public function testShareWorkspaceBucket(): void
    {
        $stage = 'in';
        $bucketName = 'test-bucket-workspace-share';

        $this->dropBucketIfExists($this->_client, $stage.'.'.$bucketName, true);

        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace([], true);
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $backend->createTable('WORKSPACE_TABLE', ['id' => 'INT', 'description' => 'STRING']);

        $registeredBucketId = $this->_client->registerBucket(
            $bucketName,
            [$workspace['connection']['database'], $workspace['connection']['schema']],
            $stage,
            'I\'am in workspace',
            'snowflake',
            $bucketName,
        );

        $tables = $this->_client->listTables($stage.'.'.$bucketName);
        $this->assertCount(1, $tables);

        $shareToken = $this->linkingClient->verifyToken();
        $targetProjectId = $shareToken['owner']['id'];

        $this->shareClient->shareBucketToProjects($registeredBucketId, [$targetProjectId]);

        $sharedBucket = $this->_client->getBucket($registeredBucketId);
        $this->assertEquals('specific-projects', $sharedBucket['sharing']);
        $this->assertEquals($targetProjectId, $sharedBucket['sharingParameters']['projects'][0]['id']);

        $this->shareClient->unshareBucket($registeredBucketId);
        $unsharedBucket = $this->_client->getBucket($registeredBucketId);
        $this->assertNull($unsharedBucket['sharing']);

        $workspaces->deleteWorkspace($workspace['id']);
    }
}
