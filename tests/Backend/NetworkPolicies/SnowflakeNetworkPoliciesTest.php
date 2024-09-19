<?php

declare(strict_types=1);

namespace Keboola\Test\Backend\NetworkPolicies;

use Keboola\Db\Import\Exception;
use Keboola\StorageApi\Workspaces;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;

class SnowflakeNetworkPoliciesTest extends NetworkPoliciesTestCase
{
    public function testAccessWithoutSystemNetworkPolicy(): void
    {
        $systemNetworkPolicyName = $this->defaultNetworkPolicyName();
        $this->assertNetworkPolicyNotExists($systemNetworkPolicyName);

        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace(['backend' => 'snowflake'], true);

        $this->assertDontHaveNetworkPolicyEnabled($workspace['connection']['user'], $systemNetworkPolicyName);

        $workspaceBackend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        // expect no exception so workspace have still access to Snowflake
        $workspaceBackend->createTable('NP_TEST_TABLE', ['ID' => 'INT', 'LASTNAME' => 'VARCHAR(255)']);
        $workspaceBackend->dropTable('NP_TEST_TABLE');

        $workspace2 = $workspaces->createWorkspace(['backend' => 'snowflake', 'networkPolicy' => 'system'], true);

        $this->assertDontHaveNetworkPolicyEnabled($workspace2['connection']['user'], $systemNetworkPolicyName);

        $workspace2Backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace2);

        // expect no exception so workspace have still access to Snowflake
        $workspace2Backend->createTable('NP_TEST_TABLE', ['ID' => 'INT', 'LASTNAME' => 'VARCHAR(255)']);
        $workspace2Backend->dropTable('NP_TEST_TABLE');

        $workspaces->deleteWorkspace($workspace['id']);
        $workspaces->deleteWorkspace($workspace2['id']);
    }

    public function testAccessWithSystemNetworkPolicy(): void
    {
        $systemNetworkPolicyName = $this->defaultNetworkPolicyName();

        if ($this->networkPolicyExists($systemNetworkPolicyName)) {
            $this->dropNetworkPolicy($systemNetworkPolicyName);
        }
        $this->createNetworkPolicyWithMyPublicIp($systemNetworkPolicyName);

        $workspaces = new Workspaces($this->_client);
        $workspace1 = $workspaces->createWorkspace(['backend' => 'snowflake'], true);

        $this->assertDontHaveNetworkPolicyEnabled($workspace1['connection']['user'], $systemNetworkPolicyName);

        $workspace1Backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace1);

        // expect no exception so workspace have still access to Snowflake
        $workspace1Backend->createTable('NP_TEST_TABLE', ['ID' => 'INT', 'LASTNAME' => 'VARCHAR(255)']);
        $workspace1Backend->dropTable('NP_TEST_TABLE');

        $workspace2 = $workspaces->createWorkspace(['backend' => 'snowflake', 'networkPolicy' => 'system'], false);

        $this->assertHaveNetworkPolicyEnabled($workspace2['connection']['user'], $systemNetworkPolicyName);

        $workspace2Backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace2);

        // expect no exception so workspace have still access to Snowflake
        $workspace2Backend->createTable('NP_TEST_TABLE', ['ID' => 'INT', 'LASTNAME' => 'VARCHAR(255)']);
        $workspace2Backend->dropTable('NP_TEST_TABLE');

        $workspaces->deleteWorkspace($workspace1['id']);
        $workspaces->deleteWorkspace($workspace2['id']);

        $this->dropNetworkPolicy($systemNetworkPolicyName);
    }

    public function testAccessWithPrivateIpInNetworkPolicy(): void
    {
        $systemNetworkPolicyName = $this->defaultNetworkPolicyName();

        if ($this->networkPolicyExists($systemNetworkPolicyName)) {
            $this->dropNetworkPolicy($systemNetworkPolicyName);
        }
        $this->createNetworkPolicyWithPrivateIp($systemNetworkPolicyName);

        $workspaces = new Workspaces($this->_client);
        $workspace1 = $workspaces->createWorkspace(['backend' => 'snowflake'], true);

        $this->assertDontHaveNetworkPolicyEnabled($workspace1['connection']['user'], $systemNetworkPolicyName);

        $workspace1Backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace1);

        // expect no exception so workspace have still access to Snowflake
        $workspace1Backend->createTable('NP_TEST_TABLE', ['ID' => 'INT', 'LASTNAME' => 'VARCHAR(255)']);
        $workspace1Backend->dropTable('NP_TEST_TABLE');

        $workspace2 = $workspaces->createWorkspace(['backend' => 'snowflake', 'networkPolicy' => 'system'], false);

        $this->assertHaveNetworkPolicyEnabled($workspace2['connection']['user'], $systemNetworkPolicyName);

        try {
            WorkspaceBackendFactory::createWorkspaceBackend($workspace2);
            $this->fail('This should fail on "not allowed access to Snowflake"');
        } catch (Exception $exception) {
            $this->assertTrue(
                str_contains('is not allowed to access Snowflake', $exception->getMessage()),
                sprintf('User %s have still access to Snowflake', $workspace2['connection']['user']),
            );
        }

        $workspaces->deleteWorkspace($workspace1['id']);
        $workspaces->deleteWorkspace($workspace2['id']);

        $this->dropNetworkPolicy($systemNetworkPolicyName);
    }
}
