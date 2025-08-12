<?php

declare(strict_types=1);

namespace Keboola\Test\Backend\Workspaces;

use Exception;
use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApi\Workspaces;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Keboola\Test\Backend\WorkspaceConnectionTrait;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;
use Keboola\Test\ClientProvider\ClientProvider;
use Keboola\Test\Utils\EventsQueryBuilder;

class WorkspacesCredentialsTest extends ParallelWorkspacesTestCase
{
    use WorkspaceConnectionTrait;

    public function setUp(): void
    {
        parent::setUp();

        $this->allowTestForBackendsOnly(['snowflake', 'bigquery'], 'Workspace credentials are implemented only for Snowflake and BigQuery');
    }

    public function testConnectByCredentials(): void
    {
        $this->skipTestForBackend(['bigquery'], 'BigQuery is WIP');

        // create workspace and source table in workspace
        $workspace = $this->initTestWorkspace();

        // connect to workspace after creation (we have credentials in response)
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        // create dummy table to check it later by
        $backend->dropTableIfExists('test_Languages');
        $backend->createTable('test_Languages', [
            'id' => 'integer',
            'name' => 'string',
        ]);
        if ($workspace['connection']['backend'] === 'snowflake') {
            $backend->executeQuery(sprintf(
                'INSERT INTO %s (%s, %s) VALUES (1, \'cz\'), (2, \'en\');',
                SnowflakeQuote::createQuotedIdentifierFromParts([
                    $workspace['connection']['schema'],
                    'test_Languages',
                ]),
                SnowflakeQuote::quoteSingleIdentifier('id'),
                SnowflakeQuote::quoteSingleIdentifier('name'),
            ));
        } else {
            // BigQuery
            $backend->executeQuery(sprintf(
                'INSERT INTO %s.`test_Languages` (`id`, `name`) VALUES (1, \'cz\'), (2, \'en\');',
                $workspace['connection']['schema'],
            ));
        }

        $workspaces = new Workspaces($this->workspaceSapiClient);

        $this->initEvents($this->_client);
        // credentials creation
        $assertCallback = function ($events) {
            $this->assertCount(1, $events);
        };

        $query = new EventsQueryBuilder();
        $query->setEvent('storage.workspaceCredentialsCreated')
            ->setTokenId($this->tokenId);
        $this->assertEventWithRetries($this->_client, $assertCallback, $query);

        $workspaceCredentials = $workspaces->createCredentials($workspace['id']);
        $workspaceBackend = WorkspaceBackendFactory::createWorkspaceBackend($workspaceCredentials, true);

        $dbResult = $workspaceBackend->fetchAll('test_Languages');

        $this->assertEquals(
            [
                0 => [1, 'cz'],
                1 => [2, 'en'],
                ],
            $dbResult,
        );

        // should just return the same credentials
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.workspaceCredentialsRetrieved')
            ->setTokenId($this->tokenId);
        $this->assertEventWithRetries($this->_client, $assertCallback, $query);
        $retrievedCredentials = $workspaces->createCredentials($workspace['id']);
        $this->assertEquals($workspaceCredentials['connection']['private_key'], $retrievedCredentials['connection']['private_key']);

        // credential detail is working and returning a correct object
        $credentialsId = $workspaceCredentials['connection']['credentials']['id'];

        $query = new EventsQueryBuilder();
        $query->setEvent('storage.workspaceCredentialsDetail')
            ->setTokenId($this->tokenId);
        $this->assertEventWithRetries($this->_client, $assertCallback, $query);
        $workspaceCredentialsRefreshed = $workspaces->getCredentials($workspace['id'], $credentialsId);
        $this->assertEquals($workspaceCredentials['connection']['private_key'], $workspaceCredentialsRefreshed['connection']['private_key']);
        $workspaceBackendRefreshed = WOrkspaceBackendFactory::createWorkspaceBackend($workspaceCredentialsRefreshed, true);

        $dbResultRefreshed = $workspaceBackendRefreshed->fetchAll('test_Languages');

        $this->assertEquals(
            [
                0 => [1, 'cz'],
                1 => [2, 'en'],
            ],
            $dbResultRefreshed,
        );


        $query = new EventsQueryBuilder();
        $query->setEvent('storage.workspaceCredentialsDeleted')
            ->setTokenId($this->tokenId);
        $this->assertEventWithRetries($this->_client, $assertCallback, $query);
        $workspaces->deleteCredentials($workspace['id'], $credentialsId);

        try {
            WorkspaceBackendFactory::createWorkspaceBackend($workspaceCredentialsRefreshed, true);
            $this->fail('Expected exception to be thrown.');
        } catch (Exception $e) {
            $this->assertTrue(str_contains($e->getMessage(), 'JWT token is invalid.'));
        }
    }
}
