<?php

namespace Keboola\Test;

use Keboola\StorageApi\Client;
use PHPUnit\Framework\TestCase;

class ClientTestCase extends TestCase
{
    /**
     * @return Client
     */
    protected function getClient(array $options)
    {
        $testSuiteName = '';
        if (SUITE_NAME) {
            $testSuiteName = sprintf('Suite: %s, ', getenv('SUITE_NAME'));
        }

        $buildId = '';
        if (TRAVIS_BUILD_ID) {
            $buildId = sprintf('Build id: %s, ', getenv('TRAVIS_BUILD_ID'));
        }

        $tokenParts = explode('-', $options['token']);
        $options['userAgent'] = sprintf(
            '%s%sStack: %s, Project: %s, Token: %s, Test: %s',
            $buildId,
            $testSuiteName,
            $options['url'],
            $tokenParts[1],
            $tokenParts[0],
            $this->getTestName()
        );
        return new Client($options);
    }

    /**
     * @return string
     */
    protected function getTestName()
    {
        return get_class($this) . '::' . $this->getName();
    }

    /**
     * @return Client
     */
    protected function getDefaultClient()
    {
        return $this->getClient([
            'token' => STORAGE_API_TOKEN,
            'url' => STORAGE_API_URL,
            'backoffMaxTries' => 1,
            'jobPollRetryDelay' => function () {
                return 1;
            },
        ]);
    }

    /**
     * @return Client
     */
    protected function getGuestStorageApiClient()
    {
        return $this->getClient([
            'token' => STORAGE_API_GUEST_TOKEN,
            'url' => STORAGE_API_URL,
            'backoffMaxTries' => 1,
            'jobPollRetryDelay' => function () {
                return 1;
            },
        ]);
    }

    /**
     * @param string $token
     * @return Client
     */
    public function getClientForToken($token)
    {
        return $this->getClient([
            'token' => $token,
            'url' => STORAGE_API_URL,
            'backoffMaxTries' => 1,
            'jobPollRetryDelay' => function () {
                return 1;
            },
        ]);
    }
}
