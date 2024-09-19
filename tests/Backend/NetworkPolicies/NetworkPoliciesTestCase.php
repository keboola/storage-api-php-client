<?php

declare(strict_types=1);

namespace Keboola\Test\Backend\NetworkPolicies;

use Doctrine\DBAL\Exception\DriverException;
use Keboola\StorageDriver\Shared\Utils\StringUtils;
use Keboola\Test\StorageApiTestCase;
use Keboola\Test\Utils\ConnectionUtils;
use Keboola\Test\Utils\IpUtils;
use Throwable;

class NetworkPoliciesTestCase extends StorageApiTestCase
{
    use ConnectionUtils;
    use IpUtils;

    private function getDBPrefix(): string
    {
        $dbPrefix = getenv('SNOWFLAKE_DB_PREFIX');
        assert($dbPrefix !== false, 'SNOWFLAKE_DB_PREFIX env var is not set');
        return $dbPrefix;
    }

    protected function useDatabase(string $database): void
    {
        $db = $this->ensureSnowflakeConnection();
        $db->executeQuery(sprintf('USE DATABASE %s;', $database));
    }

    /**
     * Keep in sync with \Keboola\Connection\Storage\Service\Backend\NameGenerator\SnowflakeObjectNameGenerator::defaultNetworkPolicyName
     */
    protected function defaultNetworkPolicyName(): string
    {
        return strtoupper(sprintf(
            '%s_SYSTEM_IPS_ONLY',
            rtrim($this->getDBPrefix(), '_'),
        ));
    }

    /**
     * Keep in sync with \Keboola\Connection\Storage\Service\Backend\NameGenerator\SnowflakeObjectNameGenerator::organizationNetworkPolicyName
     */
    protected function organizationNetworkPolicyName(int $organizationId): string
    {
        return strtoupper(sprintf(
            '%s_ORG_%d_IPS',
            rtrim($this->getDBPrefix(), '_'),
            $organizationId,
        ));
    }

    protected function createNetworkPolicyWithMyPublicIp(string $database, string $networkPolicyName): void
    {
        $ip = $this->getMyPublicIp();
        $this->createNetworkPolicyForIp($ip, $database, $networkPolicyName);
    }

    protected function createNetworkPolicyWithPrivateIp(string $database, string $networkPolicyName): void
    {
        $this->createNetworkPolicyForIp('10.0.0.1', $database, $networkPolicyName);
    }

    protected function createNetworkPolicyForIp(string $ip, string $database, string $networkPolicyName): void
    {
        $db = $this->ensureSnowflakeConnection();
        $this->useDatabase($database);

        $networkRuleName = $this->defaultNetworkRuleName();
        $this->createNetworkRuleWithIp($database, $networkRuleName, $ip);

        $db->executeQuery(sprintf(
            'CREATE OR REPLACE NETWORK POLICY %s ALLOWED_NETWORK_RULE_LIST = (%s)',
            $db->quoteIdentifier($networkPolicyName),
            $db->quoteIdentifier($networkRuleName),
        ));
    }

    protected function dropNetworkPolicy(string $networkPolicyName): void
    {
        $db = $this->ensureSnowflakeConnection();

        /** @var array<array{value: string}> $networkPolicies */
        $networkPolicies = $db->fetchAllAssociative(sprintf(
            'DESCRIBE NETWORK POLICY %s',
            $db->quoteIdentifier($networkPolicyName),
        ));

        $retryCount = 0;
        do {
            try {
                $db->executeQuery(sprintf(
                    'DROP NETWORK POLICY IF EXISTS %s',
                    $db->quoteIdentifier($networkPolicyName),
                ));
                break;
            } catch (DriverException $ex) {
                if (str_contains($ex->getMessage(), 'Cannot perform Drop operation on network policy')) {
                    preg_match('/The policy is attached to USER with name (?<username>[A-Z0-9_]+)./', $ex->getMessage(), $matches);
                    $username = $matches['username'] ?? null;
                    if ($username !== null) {
                        $db->executeQuery(sprintf(
                            'ALTER USER %s UNSET NETWORK_POLICY',
                            $db->quoteIdentifier($username),
                        ));
                        $retryCount++;
                        continue;
                    }
                }
                throw $ex;
            }
        } while ($retryCount < 10);

        foreach ($networkPolicies as $networkPolicy) {
            /** @var array<array{fullyQualifiedRuleName: string}> $values */
            $values = json_decode($networkPolicy['value'], true);
            foreach ($values as $value) {
                $db->executeQuery(sprintf(
                    'DROP NETWORK RULE IF EXISTS %s',
                    $db->quoteIdentifier($value['fullyQualifiedRuleName']),
                ));
            }
        }
    }

    protected function networkPolicyExists(string $networkPolicyName): bool
    {
        $db = $this->ensureSnowflakeConnection();

        try {
            $db->fetchAllAssociative(sprintf('DESCRIBE NETWORK POLICY %s;', $networkPolicyName));
            return true;
        } catch (DriverException $ex) {
            return false;
        }
    }

    protected function assertNetworkPolicyExists(string $networkPolicyName): void
    {
        $this->assertTrue(
            $this->networkPolicyExists($networkPolicyName),
            'Network policy should exist.',
        );
    }

    protected function assertNetworkPolicyNotExists(string $networkPolicyName): void
    {
        $this->assertFalse(
            $this->networkPolicyExists($networkPolicyName),
            'Network policy should not exists.',
        );
    }

    protected function haveNetworkPolicyEnabled(string $username, string $networkPolicyName): bool
    {
        $db = $this->ensureSnowflakeConnection();

        /** @var array<array{granted_by: string}> $userGrants */
        $userGrants = $db->fetchAllAssociative(sprintf(
            'SHOW GRANTS ON USER %s',
            $db->quoteIdentifier($username),
        ));

        $ownerRole = $userGrants[0]['granted_by'];

        /** @var array<array{'CURRENT_ROLE': string}> $currentRoleRow */
        $currentRoleRow = $db->fetchAllAssociative('SELECT CURRENT_ROLE() AS CURRENT_ROLE');
        $currentRoleName = $currentRoleRow[0]['CURRENT_ROLE'];

        try {
            $db->executeQuery(sprintf(
                'GRANT ROLE %s TO ROLE %s',
                $db->quoteIdentifier($ownerRole),
                $db->quoteIdentifier($this->getSnowflakeUser()),
            ));
        } catch (DriverException $ex) {
            $this->fail(
                sprintf(
                    'Fail to grant: GRANT ROLE %s TO ROLE %s. %s',
                    $db->quoteIdentifier($ownerRole),
                    $db->quoteIdentifier($this->getSnowflakeUser()),
                    $ex->getMessage(),
                ),
            );
        }

        $db->executeQuery(sprintf(
            'USE ROLE %s',
            $db->quoteIdentifier($ownerRole),
        ));

        $sql = sprintf(
            'SHOW PARAMETERS LIKE \'network_policy\' IN USER %s',
            $db->quoteIdentifier($username),
        );
        $networkPolicy = $db->fetchAllAssociative($sql);

        $db->executeQuery(sprintf(
            'USE ROLE %s',
            $db->quoteIdentifier($currentRoleName),
        ));

        return ($networkPolicy[0]['value'] ?? null) === $networkPolicyName;
    }

    protected function assertHaveNetworkPolicyEnabled(string $username, string $networkPolicyName): void
    {
        $this->assertTrue(
            $this->haveNetworkPolicyEnabled($username, $networkPolicyName),
            'User DON\'T have network policy enabled.',
        );
    }

    protected function assertDontHaveNetworkPolicyEnabled(string $username, string $networkPolicyName): void
    {
        $this->assertFalse(
            $this->haveNetworkPolicyEnabled($username, $networkPolicyName),
            'User HAVE network policy enabled.',
        );
    }

    protected function defaultNetworkRuleName(): string
    {
        return 'ALLOW_SYSTEM_IPS';
    }

    protected function createNetworkRuleWithIp(string $database, string $name, string $ip): void
    {
        $this->useDatabase($database);
        $db = $this->ensureSnowflakeConnection();

        $db->executeQuery(sprintf(
            'CREATE OR REPLACE NETWORK RULE %s TYPE = IPV4 VALUE_LIST = (%s)',
            $db->quoteIdentifier($name),
            $db->quoteIdentifier($ip),
        ));
    }
}
