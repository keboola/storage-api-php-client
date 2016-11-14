<?php
/**
 * Created by JetBrains PhpStorm.
 * User: martinhalamicek
 * Date: 23/01/14
 * Time: 15:02
 * To change this template use File | Settings | File Templates.
 */

namespace Keboola\StorageApi\Options;

class FileUploadOptions
{

    private $fileName;
    private $notify = false;
    private $isPublic = false;
    private $tags = array();
    private $compress = false;
    private $federationToken = false;
    private $sizeBytes;
    private $isPermanent = false;
    private $isSliced = false;
    private $isEncrypted = true;

    /**
     * @return mixed
     */
    public function getFileName()
    {
        return $this->fileName;
    }

    /**
     * @param $fileName
     * @return $this
     */
    public function setFileName($fileName)
    {
        $this->fileName = $fileName;
        return $this;
    }

    /**
     * @return boolean
     */
    public function getNotify()
    {
        return $this->notify;
    }

    /**
     * @param $notify
     * @return $this
     */
    public function setNotify($notify)
    {
        $this->notify = $notify;
        return $this;
    }

    /**
     * @return boolean
     */
    public function getIsPublic()
    {
        return $this->isPublic;
    }

    /**
     * @param $isPublic
     * @return $this
     */
    public function setIsPublic($isPublic)
    {
        $this->isPublic = $isPublic;
        return $this;
    }

    /**
     * @return array
     */
    public function getTags()
    {
        return $this->tags;
    }

    /**
     * @param array $tags
     * @return $this
     */
    public function setTags(array $tags)
    {
        $this->tags = $tags;
        return $this;
    }

    /**
     * @return boolean
     */
    public function getCompress()
    {
        return $this->compress;
    }

    /**
     * @param $compress
     * @return $this
     */
    public function setCompress($compress)
    {
        $this->compress = $compress;
        return $this;
    }

    /**
     * @return bool
     */
    public function getFederationToken()
    {
        return $this->federationToken;
    }

    /**
     * @param $federationToken
     * @return $this
     */
    public function setFederationToken($federationToken)
    {
        $this->federationToken = (bool)$federationToken;
        return $this;
    }

    /**
     * @return int
     */
    public function getSizeBytes()
    {
        return $this->sizeBytes;
    }

    /**
     * @param $sizeBytes
     * @return $this
     */
    public function setSizeBytes($sizeBytes)
    {
        $this->sizeBytes = (int)$sizeBytes;
        return $this;
    }

    /**
     * @return boolean
     */
    public function getIsPermanent()
    {
        return $this->isPermanent;
    }

    /**
     * @param $permanent
     * @return $this
     */
    public function setIsPermanent($permanent)
    {
        $this->isPermanent = (bool)$permanent;
        return $this;
    }

    /**
     * @return boolean
     */
    public function getIsSliced()
    {
        return $this->isSliced;
    }

    /**
     * @param $isSliced
     * @return $this
     */
    public function setIsSliced($isSliced)
    {
        $this->isSliced = $isSliced;
        return $this;
    }

    /**
     * @return boolean
     */
    public function getIsEncrypted()
    {
        return $this->isEncrypted;
    }

    /**
     * @param $encrypted
     * @return $this
     */
    public function setIsEncrypted($encrypted)
    {
        $this->isEncrypted = (bool)$encrypted;
        return $this;
    }
}
