<?php
/**
 * Created by JetBrains PhpStorm.
 * User: martinhalamicek
 * Date: 23/01/14
 * Time: 15:02
 * To change this template use File | Settings | File Templates.
 */

namespace Keboola\StorageApi\Options;


class FileUploadOptions {

	private $fileName;
	private $notify = true;
	private $isPublic = false;
	private $tags = array();
	private $compress = false;

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

}