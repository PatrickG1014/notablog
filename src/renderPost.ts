import fs, { promises as fsPromises } from 'fs'
import path from 'path'
import crypto from 'crypto'
import https from 'https'
import http from 'http'
import visit from 'unist-util-visit'
import { getOnePageAsTree } from 'nast-util-from-notionapi'
import { renderToHTML } from 'nast-util-to-react'
import type {
  FormattingLink,
  FormattingMentionResource,
  Resource,
  SemanticString,
} from 'nast-types'

import { toDashID } from './utils/notion'
import { log } from './utils/misc'
import { RenderPostTask, SiteContext } from './types'

function isFormattingLink(mark: unknown): mark is FormattingLink {
  return (
    Array.isArray(mark) &&
    mark.length >= 2 &&
    mark[0] === 'a' &&
    typeof mark[1] === 'string'
  )
}

function getInlineMentionResource(
  semanticString: SemanticString
): Resource | undefined {
  if (semanticString[0] !== '‣') return undefined
  const formats = semanticString[1]
  if (!Array.isArray(formats)) return undefined
  for (const format of formats) {
    const candidate = format as FormattingMentionResource
    if (
      Array.isArray(candidate) &&
      candidate[0] === 'p' &&
      candidate[1] &&
      typeof candidate[1].uri === 'string'
    ) {
      return candidate[1]
    }
  }
  return undefined
}

function createLinkTransformer(siteContext: SiteContext) {
  /** Get no dash page id. */
  function getPageIdFromUri(uri: string): string | undefined {
    return uri.split('/').pop()
  }

  /** Replace internal links for a node. */
  return function (node: NAST.Block, _index: number, parent: NAST.Block) {
    /** Skip root. */
    if (!parent) return

    /** Link to page. */
    if (node.type === 'page') {
      const pageId = getPageIdFromUri(node.uri)
      if (!pageId) return

      const page = siteContext.pages.find(page => page.id === pageId)
      if (!page) return

      log.debug(`Replace link: ${node.uri} -> ${page.url}`)
      node.uri = page.url

      return
    }

    /** Inline mention or link. */
    /** `node` may be any block with text, specifying text block here is 
        to eliminate type errors.  */
    const richTextStrs = (node as NAST.Text).title || []
    for (let i = 0; i < richTextStrs.length; i++) {
      const richTextStr = richTextStrs[i] as SemanticString

      /** Inline mention page. */
      const pageInline = getInlineMentionResource(richTextStr)
      if (pageInline) {
        const pageId = getPageIdFromUri(pageInline.uri)
        if (!pageId) continue

        const page = siteContext.pages.find(page => page.id === pageId)
        if (page) {
          log.debug(`Replace link: ${pageInline.uri} -> ${page.url}`)
          pageInline.uri = page.url
        } else {
          const newLink = `https://www.notion.so/${pageId}`
          pageInline.uri = newLink
          log.debug(`Replace link: ${pageInline.uri} -> ${newLink}`)
        }

        continue
      }

      if (Array.isArray(richTextStr[1]))
        richTextStr[1].forEach(mark => {
          if (isFormattingLink(mark)) {
            /** Inline link to page or block. */
            /**
             * Link to a page:
             * '/65166b7333374374b13b040ca1599593'
             *
             * Link to a block in a page:
             * '/ec83369b2a9c438093478ddbd8da72e6#aa3f7c1be80d485499910685dee87ba9'
             *
             * Link to a page in a collection view, the page is opened in
             * preview mode (not supported):
             * '/595365eeed0845fb9f4d641b7b845726?v=a1cb648704784afea1d5cdfb8ac2e9f0&p=65166b7333374374b13b040ca1599593'
             */
            const toPath = mark[1]
            if (!toPath) return

            /** Ignore non-notion-internal links. */
            if (!toPath.startsWith('/')) return

            /** Ignore unsupported links. */
            if (toPath.includes('?')) {
              const newPath = `https://www.notion.so${toPath}`
              log.debug(`Replace link: ${toPath} -> ${newPath}`)
              mark[1] = newPath
              return
            }

            const ids = toPath.replace(/\//g, '').split('#')

            if (ids.length > 0) {
              const targetPage = ids[0]
              const targetBlock = ids[1]
              const pageInfo = siteContext.pages.find(
                page => page.id === targetPage
              )

              if (pageInfo) {
                /** The page is in the table. */
                const newLink = `${pageInfo.url}${
                  targetBlock ? '#https://www.notion.so/' + targetBlock : ''
                }`
                mark[1] = newLink
              } else {
                /** The page is not in the table. */
                const newLink = `https://www.notion.so${toPath}`
                mark[1] = newLink
              }

              log.debug(`Replace link: ${toPath} -> ${mark[1]}`)
              return
            }
          }
        })
    }

    return
  }
}

/**
 * Render a post.
 * @param task
 */
export async function renderPost(task: RenderPostTask): Promise<number> {
  try {
    const { doFetchPage, pageMetadata, siteContext } = task.data
    const { cache, notionAgent, renderer } = task.tools
    const config = task.config

    const pageID = toDashID(pageMetadata.id)
    let tree: NAST.Block

    /** Fetch page. */
    if (doFetchPage) {
      log.info(`Fetch data of page "${pageID}"`)

      tree = await getOnePageAsTree(pageID, notionAgent)
      /** Use internal links for pages in the table. */
      visit(
        tree as unknown as import('unist').Node,
        createLinkTransformer(
          siteContext
        ) as unknown as import('unist-util-visit').Visitor<import('unist').Node>
      )
      cache.set('notion', pageID, tree)

      log.info(`Cache of "${pageID}" is saved`)
    } else {
      log.info(`Read cache of page "${pageID}"`)

      const cachedTree = cache.get('notion', pageID)

      if (cachedTree != null) tree = cachedTree as NAST.Block
      else
        throw new Error(`\
Cache of page "${pageID}" is corrupted, run "notablog generate --fresh <path_to_starter>" to fix`)
    }

    await resolveAttachmentImages(tree, config.outDir)

    /** Render with template. */
    if (pageMetadata.publish) {
      log.info(`Render published page "${pageID}"`)

      const outDir = config.outDir
      const outPath = path.join(outDir, pageMetadata.url)

      const contentHTML = renderToHTML(tree)
      const pageHTML = renderer.render(pageMetadata.template, {
        siteMeta: siteContext,
        post: {
          ...pageMetadata,
          contentHTML,
        },
      })

      await fsPromises.writeFile(outPath, pageHTML, { encoding: 'utf-8' })
      return 0
    } else {
      log.info(`Skip rendering of unpublished page "${pageID}"`)
      return 1
    }
  } catch (error) {
    log.error(error)
    return 2
  }
}

function isAttachmentUrl(url: string): boolean {
  return url.startsWith('attachment:')
}

function extractBlockIdFromUri(uri: string): string | undefined {
  try {
    const url = new URL(uri)
    const last = (url.pathname.split('/').pop() || '').split('?')[0]
    if (!last) return undefined
    return toDashID(last)
  } catch {
    return undefined
  }
}

function extractAttachmentFileName(attachmentUrl: string): string | undefined {
  const withoutQuery = attachmentUrl.split('?')[0]
  const parts = withoutQuery.split(':')
  if (parts.length < 3) return undefined
  return parts.slice(2).join(':')
}

function hashString(input: string): string {
  return crypto.createHash('sha1').update(input).digest('hex')
}

async function getSignedFileUrls(
  requests: { url: string; permissionRecord: { table: 'block'; id: string } }[]
): Promise<Map<string, string>> {
  if (requests.length === 0) return new Map()
  const body = JSON.stringify({ urls: requests })
  const options = {
    method: 'POST',
    hostname: 'www.notion.so',
    path: '/api/v3/getSignedFileUrls',
    headers: {
      'Content-Type': 'application/json',
      'Content-Length': Buffer.byteLength(body),
    },
  }

  const response = await new Promise<unknown>((resolve, reject) => {
    const req = https.request(options, res => {
      let data = ''
      res.on('data', chunk => (data += chunk))
      res.on('end', () => {
        try {
          resolve(JSON.parse(data))
        } catch (error) {
          reject(error)
        }
      })
    })
    req.on('error', reject)
    req.write(body)
    req.end()
  })

  const map = new Map<string, string>()
  const signedUrls =
    (response as { signedUrls?: { url: string; signedUrl: string }[] })
      .signedUrls || []
  signedUrls.forEach(item => {
    if (item?.url && item?.signedUrl) {
      map.set(item.url, item.signedUrl)
    }
  })
  if (signedUrls.length === 0) {
    log.warn('No signed URLs returned from Notion API')
  }
  return map
}

async function downloadToFile(url: string, filePath: string): Promise<void> {
  await fsPromises.mkdir(path.dirname(filePath), { recursive: true })
  if (fs.existsSync(filePath)) return

  const client = url.startsWith('https:') ? https : http
  await new Promise<void>((resolve, reject) => {
    const request = client.get(url, response => {
      if (response.statusCode && response.statusCode >= 400) {
        reject(new Error(`Failed to download ${url}: ${response.statusCode}`))
        return
      }
      const fileStream = fs.createWriteStream(filePath)
      response.pipe(fileStream)
      fileStream.on('finish', () => fileStream.close(() => resolve()))
      fileStream.on('error', reject)
    })
    request.on('error', reject)
  })
}

async function resolveAttachmentImages(
  tree: NAST.Block,
  outDir: string
): Promise<void> {
  const attachments: {
    node: NAST.Image
    blockId: string
    blockIdNoDash: string
    attachmentUrl: string
  }[] = []

  visit(tree, node => {
    if (node && node.type === 'image') {
      const image = node as NAST.Image
      if (typeof image.source === 'string' && isAttachmentUrl(image.source)) {
        const blockId = extractBlockIdFromUri(image.uri)
        if (blockId) {
          attachments.push({
            node: image,
            blockId,
            blockIdNoDash: blockId.replace(/-/g, ''),
            attachmentUrl: image.source,
          })
        }
      }
    }
  })

  if (attachments.length === 0) return

  const signedUrlMap = await getSignedFileUrls(
    attachments.flatMap(item => {
      const baseUrl = item.attachmentUrl.split('?')[0]
      const urls =
        baseUrl === item.attachmentUrl
          ? [item.attachmentUrl]
          : [item.attachmentUrl, baseUrl]
      return urls.flatMap(url => [
        {
          url,
          permissionRecord: { table: 'block' as const, id: item.blockId },
        },
        {
          url,
          permissionRecord: { table: 'block' as const, id: item.blockIdNoDash },
        },
      ])
    })
  )

  const assetsDir = path.join(outDir, 'assets', 'notion')
  for (const item of attachments) {
    const attachmentBase = item.attachmentUrl.split('?')[0]
    const signedUrl =
      signedUrlMap.get(item.attachmentUrl) || signedUrlMap.get(attachmentBase)
    if (!signedUrl) {
      log.warn(
        `No signed URL for attachment: ${item.attachmentUrl} (block ${item.blockId})`
      )
      continue
    }
    const attachmentFileName = extractAttachmentFileName(item.attachmentUrl)
    const ext =
      (attachmentFileName && path.extname(attachmentFileName)) ||
      path.extname(new URL(signedUrl).pathname) ||
      '.bin'
    const fileName = `${hashString(item.attachmentUrl)}${ext}`
    const outPath = path.join(assetsDir, fileName)
    await downloadToFile(signedUrl, outPath)
    item.node.source = `/assets/notion/${fileName}`
  }
}
