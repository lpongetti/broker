.PHONY: push

# Funzione push che crea un nuovo tag incrementando il numero finale
push:
	@echo "Getting last tag..."
	@LAST_TAG=$$(git tag --sort=-version:refname | head -1); \
	if [ -z "$$LAST_TAG" ]; then \
		echo "No tags found, creating initial tag v1.0.0"; \
		NEW_TAG="v1.0.0"; \
	else \
		echo "Last tag: $$LAST_TAG"; \
		# Estrae il numero finale dal tag (es. v1.0.55 -> 55) \
		LAST_NUMBER=$$(echo $$LAST_TAG | sed 's/.*\.\([0-9]*\)$$/\1/'); \
		# Incrementa il numero \
		NEW_NUMBER=$$((LAST_NUMBER + 1)); \
		# Estrae la parte iniziale del tag (es. v1.0) \
		TAG_PREFIX=$$(echo $$LAST_TAG | sed 's/\.[0-9]*$$//'); \
		# Crea il nuovo tag \
		NEW_TAG="$$TAG_PREFIX.$$NEW_NUMBER"; \
		echo "New tag: $$NEW_TAG"; \
	fi; \
	echo "Pushing code..."; \
	git push; \
	echo "Creating and pushing tag $$NEW_TAG..."; \
	git tag $$NEW_TAG; \
	git push origin $$NEW_TAG; \
	echo "Done! Tag $$NEW_TAG created and pushed."

