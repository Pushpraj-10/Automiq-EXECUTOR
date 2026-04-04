export type ActionType = 'http_request' | 'send_email' | 'webhook_notification' | 'delay';

export type OnFailurePolicy = {
	strategy?: 'retry' | 'stop' | 'continue';
	maxAttempts?: number;
};

export type QueuedAction = {
	id: string;
	stepIndex: number;
	type: ActionType;
	name?: string;
	config: Record<string, unknown>;
	onFailure?: OnFailurePolicy;
};

export function parseQueuedAction(input: {
	id?: string;
	stepIndex?: number;
	type?: string;
	name?: string;
	config?: unknown;
	onFailure?: unknown;
	fallbackIndex: number;
}): QueuedAction {
	const type = String(input.type || '');
	if (!isActionType(type)) {
		throw new Error(`Invalid action type: ${type}`);
	}

	const stepIndex = Number(input.stepIndex || input.fallbackIndex);
	if (!Number.isInteger(stepIndex) || stepIndex < 1) {
		throw new Error(`Invalid stepIndex: ${input.stepIndex}`);
	}

	return {
		id: String(input.id || `step_${stepIndex}`),
		stepIndex,
		type,
		name: input.name ? String(input.name) : undefined,
		config: isRecord(input.config) ? input.config : {},
		onFailure: isRecord(input.onFailure) ? (input.onFailure as OnFailurePolicy) : undefined,
	};
}

function isActionType(value: string): value is ActionType {
	return value === 'http_request' || value === 'send_email' || value === 'webhook_notification' || value === 'delay';
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === 'object' && value !== null && !Array.isArray(value);
}
