<?php
/**
 * Created by JetBrains PhpStorm.
 * User: martinhalamicek
 * Date: 27/10/14
 * Time: 10:55
 * To change this template use File | Settings | File Templates.
 */

namespace Keboola\StorageApi;

use Keboola\StorageApi\Options\BucketCredentials\CredentialsCreateOptions;
use Keboola\StorageApi\Options\BucketCredentials\ListCredentialsOptions;

class BucketCredentials
{


    private $client;

    public function __construct(Client $client)
    {
        $this->client = $client;
    }


    public function createCredentials(CredentialsCreateOptions $options)
    {
        return $this->client->apiPost("storage/buckets/{$options->getBucketId()}/credentials", array(
            'name' => $options->getName(),
        ));
    }

    public function dropCredentials($id)
    {
        $this->client->apiDelete("storage/credentials/{$id}");
    }

    public function getCredentials($id)
    {
        return $this->client->apiGet("storage/credentials/{$id}");
    }


    public function listCredentials(ListCredentialsOptions $options = null)
    {
        if (!$options) {
            $options = new ListCredentialsOptions();
        }

        if ($options->getBucketId()) {
            return $this->client->apiGet("storage/buckets/{$options->getBucketId()}/credentials");
        } else {
            return $this->client->apiGet("storage/credentials");
        }
    }
}
