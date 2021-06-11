<?php

namespace Keboola\Test\Backend;

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
        } catch (\Doctrine\DBAL\Driver\PDOException $e) {
            // Synapse
            if (!in_array(
                $e->getCode(),
                [
                    //https://docs.microsoft.com/en-us/sql/odbc/reference/appendixes/appendix-a-odbc-error-codes?view=sql-server-ver15
                    '28000', // Invalid authorization specification
                    '08004', // Server rejected the connection
                ],
                true
            )) {
                $this->fail(sprintf('Unexpected error code "%s" for Synapse credentials fail.', $e->getCode()));
            }
        } catch (\PDOException $e) {
            // RS
            $this->assertEquals(7, $e->getCode());
        } catch (\Keboola\Db\Import\Exception $e) {
            $this->assertContains('Incorrect username or password was specified', $e->getMessage());
        }
    }
}
