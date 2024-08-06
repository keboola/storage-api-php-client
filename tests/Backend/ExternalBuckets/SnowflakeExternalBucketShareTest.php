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
        $STAGE = 'in';
        $BUCKET_NAME = 'test-bucket-ext-share';

        $this->dropBucketIfExists($this->_client, $STAGE.'.'.$BUCKET_NAME, true);

        $this->initEvents($this->_client);

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

        $this->_client->registerBucket(
            $BUCKET_NAME,
            [self::EXTERNAL_DB, self::EXTERNAL_SCHEMA],
            $STAGE,
            'will not fail',
            'snowflake',
            $BUCKET_NAME,
        );

        $tables = $this->_client->listTables($STAGE.'.'.$BUCKET_NAME);
        $this->assertCount(1, $tables);
        $bucket = $this->_client->refreshBucket($STAGE.'.'.$BUCKET_NAME);

        $shareToken = $this->linkingClient->verifyToken();
        $targetProjectId = $shareToken['owner']['id'];

        $this->shareClient->shareBucketToProjects($bucket['id'], [$targetProjectId]);

        $sharedBucket = $this->_client->getBucket($bucket['id']);
        $this->assertEquals('specific-projects', $sharedBucket['sharing']);
        $this->assertEquals($targetProjectId, $sharedBucket['sharingParameters']['projects'][0]['id']);

        $this->shareClient->unshareBucket($bucket['id']);
        $unsharedBucket = $this->_client->getBucket($bucket['id']);
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
        $STAGE = 'in';
        $BUCKET_NAME = 'test-bucket-workspace-share';

        $this->dropBucketIfExists($this->_client, $STAGE.'.'.$BUCKET_NAME, true);

        $this->initEvents($this->_client);

        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace([], true);
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $backend->createTable('WORKSPACE_TABLE', ['id' => 'INT', 'description' => 'STRING']);

        $this->_client->registerBucket(
            $BUCKET_NAME,
            [$workspace['connection']['database'], $workspace['connection']['schema']],
            $STAGE,
            'I\'am in workspace',
            'snowflake',
            $BUCKET_NAME,
        );

        $tables = $this->_client->listTables($STAGE.'.'.$BUCKET_NAME);
        $this->assertCount(1, $tables);
        $bucket = $this->_client->refreshBucket($STAGE.'.'.$BUCKET_NAME);

        $shareToken = $this->linkingClient->verifyToken();
        $targetProjectId = $shareToken['owner']['id'];

        $this->shareClient->shareBucketToProjects($bucket['id'], [$targetProjectId]);

        $sharedBucket = $this->_client->getBucket($bucket['id']);
        $this->assertEquals('specific-projects', $sharedBucket['sharing']);
        $this->assertEquals($targetProjectId, $sharedBucket['sharingParameters']['projects'][0]['id']);

        $this->shareClient->unshareBucket($bucket['id']);
        $unsharedBucket = $this->_client->getBucket($bucket['id']);
        $this->assertNull($unsharedBucket['sharing']);

        $workspaces->deleteWorkspace($workspace['id']);
    }
}
