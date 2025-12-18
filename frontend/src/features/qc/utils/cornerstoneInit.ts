/**
 * Cornerstone.js initialization utilities.
 *
 * Based on Cornerstone3D examples (initDemo.ts):
 * 1. Initialize DICOM image loader FIRST (registers loaders)
 * 2. Initialize cornerstone core
 * 3. Initialize cornerstone tools
 */

import {
  init as csRenderInit,
  RenderingEngine,
  Enums,
  metaData,
  utilities,
  cache,
} from '@cornerstonejs/core';
import {
  init as csToolsInit,
  addTool,
  ToolGroupManager,
  StackScrollTool,
  WindowLevelTool,
  ZoomTool,
  PanTool,
  Enums as ToolEnums,
} from '@cornerstonejs/tools';
import cornerstoneDICOMImageLoader from '@cornerstonejs/dicom-image-loader';

let initialized = false;
let initPromise: Promise<void> | null = null;

/**
 * Initialize Cornerstone.js libraries (only runs once).
 * Follows the exact order from Cornerstone3D examples.
 */
export async function initCornerstone(): Promise<void> {
  // Return resolved promise if already initialized
  if (initialized) {
    return Promise.resolve();
  }

  // Return existing promise if initialization is in progress
  if (initPromise) {
    return initPromise;
  }

  initPromise = (async () => {
    try {
      // Step 1: Initialize DICOM image loader FIRST (before core!)
      // This is the order used in Cornerstone3D examples (initDemo.ts)
      // Note: Web worker configuration is handled internally by the loader
      cornerstoneDICOMImageLoader.init();

      // Step 2: Add metadata providers
      const { calibratedPixelSpacingMetadataProvider } = utilities;
      if (calibratedPixelSpacingMetadataProvider) {
        metaData.addProvider(
          calibratedPixelSpacingMetadataProvider.get.bind(calibratedPixelSpacingMetadataProvider),
          11000
        );
      }

      // Step 3: Initialize cornerstone core
      await csRenderInit({
        debug: {
          statsOverlay: false,
        },
      });

      // Step 3.5: Configure cache size (1GB for smooth slice scrolling)
      // Default is ~100MB which is too small for multi-slice DICOM stacks
      cache.setMaxCacheSize(1024 * 1024 * 1024); // 1GB

      // Step 4: Initialize cornerstone tools
      await csToolsInit();

      // Step 5: Register tools globally
      addTool(StackScrollTool);
      addTool(WindowLevelTool);
      addTool(ZoomTool);
      addTool(PanTool);

      initialized = true;
    } catch (error) {
      console.error('[Cornerstone] Initialization failed:', error);
      initPromise = null;
      initialized = false;
      throw error;
    }
  })();

  return initPromise;
}

/**
 * Check if Cornerstone is initialized.
 */
export function isInitialized(): boolean {
  return initialized;
}

/**
 * Create a tool group for a viewport.
 */
export function createViewportToolGroup(toolGroupId: string, viewportId: string, renderingEngineId: string) {
  let toolGroup = ToolGroupManager.getToolGroup(toolGroupId);

  if (!toolGroup) {
    toolGroup = ToolGroupManager.createToolGroup(toolGroupId);

    if (toolGroup) {
      toolGroup.addTool(StackScrollTool.toolName);
      toolGroup.addTool(WindowLevelTool.toolName);
      toolGroup.addTool(ZoomTool.toolName);
      toolGroup.addTool(PanTool.toolName);

      // Stack scroll on mouse wheel
      toolGroup.setToolActive(StackScrollTool.toolName, {
        bindings: [{ mouseButton: ToolEnums.MouseBindings.Wheel }],
      });

      // Window/Level on left mouse button
      toolGroup.setToolActive(WindowLevelTool.toolName, {
        bindings: [{ mouseButton: ToolEnums.MouseBindings.Primary }],
      });

      // Zoom on right mouse button
      toolGroup.setToolActive(ZoomTool.toolName, {
        bindings: [{ mouseButton: ToolEnums.MouseBindings.Secondary }],
      });

      // Pan on middle mouse button
      toolGroup.setToolActive(PanTool.toolName, {
        bindings: [{ mouseButton: ToolEnums.MouseBindings.Auxiliary }],
      });
    }
  }

  if (toolGroup) {
    toolGroup.addViewport(viewportId, renderingEngineId);
  }

  return toolGroup;
}

/**
 * Clean up a tool group.
 */
export function destroyToolGroup(toolGroupId: string) {
  const toolGroup = ToolGroupManager.getToolGroup(toolGroupId);
  if (toolGroup) {
    ToolGroupManager.destroyToolGroup(toolGroupId);
  }
}

export { RenderingEngine, Enums };
