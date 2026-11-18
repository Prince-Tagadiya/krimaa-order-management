# Purva Fashion deployment

Purva shares application code with Krimaa, but uses a dedicated Firebase project (`purva-2150e`) and Netlify site (`purvafashion.netlify.app`). No Krimaa records are migrated into Purva.

Set `APP_TENANT_ID=purva`, `CLIENT_MODE=single_company`, and `CLIENT_COMPANY_NAME=PURVA FASHION` on the Purva Netlify site only. Its `FIREBASE_*` variables must reference the Purva Firebase web app. Keep `AUTH_USERS_JSON=[]` and `SHEETS_API_URL` empty. Browser configuration deliberately excludes passwords. The runtime config and client reject an incorrect Firebase project.

Enable Firebase Authentication Email/Password. Provision administrators with Firebase Auth custom claims `{ "tenant": "purva", "role": "admin" }`. Only these verified accounts can access business collections under the Purva-specific Firestore rules in this directory. There is no public registration or client-side role assignment. Deploy these rules explicitly to `purva-2150e` only; they do not apply to Krimaa.

The initial reset retained an IAM-only compressed backup of 572 old Purva documents in `_reset_backups/20261118-before-fresh-start`. Browser clients cannot read, overwrite, or delete this backup. Local recovery artifacts are ignored under `.migration/purva/`.
