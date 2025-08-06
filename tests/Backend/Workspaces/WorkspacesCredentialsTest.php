<?php

declare(strict_types=1);

namespace Keboola\Test\Backend\Workspaces;

use Keboola\StorageApi\Workspaces;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Keboola\Test\Backend\WorkspaceConnectionTrait;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;

class WorkspacesCredentialsTest extends ParallelWorkspacesTestCase
{
    use WorkspaceConnectionTrait;

    public function testConnectByCredentials(): void
    {
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
        } elseif ($workspace['connection']['backend'] === 'bigquery') {
            $backend->executeQuery(sprintf(
                'INSERT INTO %s.`test_Languages` (`id`, `name`) VALUES (1, \'cz\'), (2, \'en\');',
                $workspace['connection']['schema'],
            ));
        } else {
            $this->fail(sprintf('Unsupported workspace backend: %s', $workspace['connection']['backend']));
        }

        $workspaces = new Workspaces($this->workspaceSapiClient);

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
    }
}
