<?php

declare(strict_types=1);

namespace Keboola\Test\Backend\NetworkPolicies;

use Keboola\Db\Import\Exception;
use Keboola\StorageApi\Workspaces;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;

class SnowflakeNetworkPoliciesTest extends NetworkPoliciesTestCase
{
    public function testAccessWithoutNetworkPolicy(): void
    {
        $verifiedToken = $this->_client->verifyToken();

        $organizationNetworkPolicy = $this->organizationNetworkPolicyName($verifiedToken['organization']['id']);
        $this->assertNetworkPolicyNotExists($organizationNetworkPolicy);

        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace(['backend' => 'snowflake'], true);

        $this->assertDontHaveNetworkPolicyEnabled($workspace['connection']['user'], $organizationNetworkPolicy);

        $workspaceBackend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        // expect no exception so workspace have still access to Snowflake
        $workspaceBackend->createTable('NP_TEST_TABLE', ['ID' => 'INT', 'LASTNAME' => 'VARCHAR(255)']);
        $workspaceBackend->dropTable('NP_TEST_TABLE');

        $workspace2 = $workspaces->createWorkspace(['backend' => 'snowflake', 'networkPolicy' => 'user'], true);

        $this->assertDontHaveNetworkPolicyEnabled($workspace2['connection']['user'], $organizationNetworkPolicy);

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
        $verifiedToken = $this->_client->verifyToken();

        if (!$this->networkPolicyExists($systemNetworkPolicyName)) {
            $this->createNetworkPolicy($systemNetworkPolicyName);
        }

        // the project might not have the NP enabled -> so do it manually.
        // Keep in mind that it might not have the feature set, but it is ok
        $this->assignNetworkPolicyToProjectUser($verifiedToken['owner']['id'], $systemNetworkPolicyName);

        $this->assertNetworkPolicyExists($systemNetworkPolicyName);

        $testNetworkRuleName = $this->defaultTestsNetworkRuleName();
        $this->createNetworkRuleWithIp($testNetworkRuleName, $this->getMyPublicIp());

        $this->addNetworkRuleToNetworkPolicy($testNetworkRuleName, $systemNetworkPolicyName);

        $workspaces = new Workspaces($this->_client);
        $workspace1 = $workspaces->createWorkspace(['backend' => 'snowflake'], true);

        $this->assertDontHaveNetworkPolicyEnabled($workspace1['connection']['user'], $systemNetworkPolicyName);

        $workspace1Backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace1);

        // expect no exception so workspace have still access to Snowflake
        $workspace1Backend->createTable('NP_TEST_TABLE', ['ID' => 'INT', 'LASTNAME' => 'VARCHAR(255)']);
        $workspace1Backend->dropTable('NP_TEST_TABLE');

        $workspace2 = $workspaces->createWorkspace(['backend' => 'snowflake', 'networkPolicy' => 'system'], true);

        $this->assertHaveNetworkPolicyEnabled($workspace2['connection']['user'], $systemNetworkPolicyName);

        $workspace2Backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace2);

        // expect no exception so workspace have still access to Snowflake
        $workspace2Backend->createTable('NP_TEST_TABLE', ['ID' => 'INT', 'LASTNAME' => 'VARCHAR(255)']);
        $workspace2Backend->dropTable('NP_TEST_TABLE');

        $workspaces->deleteWorkspace($workspace1['id']);
        $workspaces->deleteWorkspace($workspace2['id']);

        $this->removeNetworkRuleFromNetworkPolicy($testNetworkRuleName, $systemNetworkPolicyName);
        $this->dropNetworkRule($testNetworkRuleName);
    }

    // this test won't be working locally because IP of local connection and IP of test runner are the same
    public function testAccessWithPrivateIpInNetworkPolicy(): void
    {
        $systemNetworkPolicyName = $this->defaultNetworkPolicyName();
        $verifiedToken = $this->_client->verifyToken();

        if (!$this->networkPolicyExists($systemNetworkPolicyName)) {
            $this->createNetworkPolicy($systemNetworkPolicyName);
        }

        // the project might not have the NP enabled -> so do it manually.
        // Keep in mind that it might not have the feature set, but it is ok
        $this->assignNetworkPolicyToProjectUser($verifiedToken['owner']['id'], $systemNetworkPolicyName);

        $this->assertNetworkPolicyExists($systemNetworkPolicyName);

        $testNetworkRuleName = $this->defaultTestsNetworkRuleName();
        $this->createNetworkRuleWithIp($testNetworkRuleName, '10.0.0.1');

        $this->addNetworkRuleToNetworkPolicy($testNetworkRuleName, $systemNetworkPolicyName);

        $workspaces = new Workspaces($this->_client);
        $workspace1 = $workspaces->createWorkspace(['backend' => 'snowflake'], true);

        $this->assertDontHaveNetworkPolicyEnabled($workspace1['connection']['user'], $systemNetworkPolicyName);

        $workspace1Backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace1);

        // expect no exception so workspace have still access to Snowflake
        $workspace1Backend->createTable('NP_TEST_TABLE', ['ID' => 'INT', 'LASTNAME' => 'VARCHAR(255)']);
        $workspace1Backend->dropTable('NP_TEST_TABLE');

        $workspace2 = $workspaces->createWorkspace(['backend' => 'snowflake', 'networkPolicy' => 'system'], true);

        $this->assertHaveNetworkPolicyEnabled($workspace2['connection']['user'], $systemNetworkPolicyName);

        try {
            WorkspaceBackendFactory::createWorkspaceBackend($workspace2);
            $this->fail('This should fail on "not allowed access to Snowflake"');
        } catch (Exception $exception) {
            $this->assertTrue(
                str_contains($exception->getMessage(), 'is not allowed to access Snowflake'),
                sprintf('User %s have still access to Snowflake', $workspace2['connection']['user']),
            );
        }

        $workspaces->deleteWorkspace($workspace1['id']);
        $workspaces->deleteWorkspace($workspace2['id']);

        $this->removeNetworkRuleFromNetworkPolicy($testNetworkRuleName, $systemNetworkPolicyName);
        $this->dropNetworkRule($testNetworkRuleName);
    }
}
