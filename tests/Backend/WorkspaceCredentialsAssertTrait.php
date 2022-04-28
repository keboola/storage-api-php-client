<?php

namespace Keboola\Test\Backend;

use Keboola\Test\StorageApiTestCase;
use Retry\BackOff\ExponentialBackOffPolicy;
use Retry\Policy\CallableRetryPolicy;
use Retry\RetryProxy;

trait WorkspaceCredentialsAssertTrait
{
    private static $RETRY_FAIL_MESSAGE = 'Credentials should be invalid';
    /**
     * @param array $connection
     * @throws \Exception
     */
    private function assertCredentialsShouldNotWork($connection)
    {
        $retryPolicy = new CallableRetryPolicy(function (\Exception $e) {
            return $e->getMessage() === self::$RETRY_FAIL_MESSAGE;
        });
        try {
            $proxy = new RetryProxy($retryPolicy, new ExponentialBackOffPolicy());
            $proxy->call(function () use ($connection) {
                $this->getDbConnection($connection);
                throw new \Exception(self::$RETRY_FAIL_MESSAGE);
            });
        } catch (\Doctrine\DBAL\Driver\Exception $e) {
            if ($connection['backend'] === StorageApiTestCase::BACKEND_SYNAPSE) {
                $this->assertStringContainsString('The login failed', $e->getMessage());
            }
            if ($connection['backend'] === StorageApiTestCase::BACKEND_EXASOL) {
                // Exasol
                if (!in_array(
                    (string) $e->getCode(),
                    [
                        '28000', // Invalid authorization specification
                        '08004', // Server rejected the connection
                    ],
                    true
                )) {
                    $this->fail(sprintf('Unexpected error code "%s" for Exasol credentials fail.', $e->getCode()));
                }
            }
        } catch (\PDOException $e) {
            // RS
            $this->assertEquals(7, $e->getCode());
        } catch (\Keboola\Db\Import\Exception $e) {
            $this->assertStringContainsString('Incorrect username or password was specified', $e->getMessage());
        } catch (\Exception $e) {
            if ($connection['backend'] === StorageApiTestCase::BACKEND_EXASOL) {
                // Exasol authentication failed
                self::assertEquals(
                    -373252,
                    $e->getCode(),
                    'Unexpected error code, expected code for Exasol is -373252.'
                );
            } else {
                throw new \Exception($e->getMessage(), (int) $e->getCode(), $e);
            }
        }
    }
}
