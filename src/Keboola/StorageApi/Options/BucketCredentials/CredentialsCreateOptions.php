<?php
/**
 * Created by JetBrains PhpStorm.
 * User: martinhalamicek
 * Date: 27/10/14
 * Time: 10:56
 * To change this template use File | Settings | File Templates.
 */

namespace Keboola\StorageApi\Options\BucketCredentials;

class CredentialsCreateOptions
{

    private $bucketId;

    private $name;

    /**
     * @return mixed
     */
    public function getBucketId()
    {
        return $this->bucketId;
    }

    /**
     * @param mixed $bucketId
     */
    public function setBucketId($bucketId)
    {
        $this->bucketId = $bucketId;
        return $this;
    }

    /**
     * @return mixed
     */
    public function getName()
    {
        return $this->name;
    }

    /**
     * @param mixed $name
     */
    public function setName($name)
    {
        $this->name = $name;
        return $this;
    }
}
